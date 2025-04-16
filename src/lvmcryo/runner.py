#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-12-03
# @Filename: runner.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import itertools
import logging
import pathlib
import signal
import sys
from datetime import timedelta

from typing import TYPE_CHECKING

import httpx
import polars

from sdsstools.utils import run_in_executor

from lvmcryo.config import Actions
from lvmcryo.handlers import LN2Handler, close_all_valves
from lvmcryo.handlers.ln2 import get_now
from lvmcryo.notifier import Notifier


if TYPE_CHECKING:
    from lvmcryo.config import Config
    from lvmcryo.tools import DBHandler


__all__ = ["ln2_runner"]


async def signal_handler(handler: LN2Handler, log: logging.Logger):
    """Handles signals to close all valves and exit cleanly."""

    log.error("User aborted the process. Closing all valves before exiting.")
    try:
        await handler.stop(only_active=False)
    except Exception as err:
        log.error(f"Failed closing LN2 valves: {err!r}")

    await handler.clear()

    handler.failed = True
    handler.aborted = True

    handler.event_times.fail_time = get_now()
    handler.event_times.abort_time = get_now()
    handler.event_times.end_time = get_now()

    log.error("Exiting now. No data or notifications will be sent.")
    sys.exit(1)


async def ln2_runner(
    handler: LN2Handler,
    config: Config,
    notifier: Notifier | None = None,
    db_handler: DBHandler | None = None,
):
    """Runs the purge/fill process.

    This function is usually called by the CLI.

    """

    if notifier is None:
        # Create a notifier but immediately disable it.
        notifier = Notifier()
        notifier.disabled = True

    log = handler.log

    # Record options used. If in no_promp is False (the default), the full
    # configuration has already been printed for confirmation, so we only
    # save this with debug level.
    config_json = config.model_dump_json(indent=2)
    log.log(
        logging.INFO if config.no_prompt else logging.DEBUG,
        f"Running {config.action.value} with configuration:\n{config_json}",
    )

    if config.dry_run:
        log.warning("Running in dry-run mode. No valves will be operated.")

    # Inform of the start of the fill in Slack.
    now_str = get_now().strftime("%H:%M:%S")

    action = config.action.value

    if config.use_thermistors or config.purge_time is None or config.fill_time is None:
        lvmweb_url = config.internal_config["notifications.lvmweb_fill_url"]
        message = f"Starting LN₂ `{action}` at {now_str}."
        if db_handler and db_handler.pk is not None:
            fill_url = lvmweb_url.format(fill_id=db_handler.pk)
            message += f" Details can be followed from the <{fill_url}|LVM webapp>."

        await notifier.post_to_slack(message)

    else:
        await notifier.post_to_slack(
            f"Starting LN₂ `{action}` at {now_str} with "
            f"purge_time={config.purge_time} and "
            f"fill_time={config.fill_time}."
        )

    max_temperature = config.max_temperature if config.check_temperatures else None
    max_pressure = config.max_pressure if config.check_pressures else None
    await handler.check(
        max_pressure=max_pressure,
        max_temperature=max_temperature,
        check_thermistors=config.use_thermistors,
    )

    # Register signals that will trigger a valve shutdown and clean exit.
    for signame in ("SIGINT", "SIGTERM"):
        asyncio.get_running_loop().add_signal_handler(
            getattr(signal, signame),
            lambda: asyncio.create_task(signal_handler(handler, log)),
        )

    log.info(f"Closing all valves before {action}.")
    await close_all_valves(dry_run=config.dry_run)

    if handler.failed or handler.aborted:
        await handler.clear()
        raise RuntimeError("The LN2 handler was aborted before starting the fill.")

    if config.action == Actions.purge_fill or config.action == Actions.purge:
        await notifier.post_to_slack("Starting purge.")
        max_purge_time = config.purge_time or config.max_purge_time
        await asyncio.wait_for(
            handler.purge(
                use_thermistor=config.use_thermistors,
                min_purge_time=config.min_purge_time,
                max_purge_time=max_purge_time,
                prompt=not config.no_prompt,
                preopen_cb=db_handler.write if db_handler is not None else None,
            ),
            timeout=max_purge_time + 60 if max_purge_time else None,
        )

        if handler.failed or handler.aborted:
            await handler.clear()
            raise RuntimeError(handler.error or "Purge failed or was aborted.")

    if config.action == Actions.purge_fill or config.action == Actions.fill:
        await notifier.post_to_slack("Starting fill.")
        max_fill_time = config.fill_time or config.max_fill_time
        await asyncio.wait_for(
            handler.fill(
                use_thermistors=config.use_thermistors,
                require_all_thermistors=config.require_all_thermistors,
                min_fill_time=config.min_fill_time,
                max_fill_time=max_fill_time,
                prompt=not config.no_prompt,
                preopen_cb=db_handler.write if db_handler is not None else None,
            ),
            timeout=max_fill_time + 60 if max_fill_time else None,
        )

        if handler.failed or handler.aborted:
            await handler.clear()
            raise RuntimeError(handler.error or "Fill failed or was aborted.")

    await notifier.post_to_slack(f"LN₂ `{action}` completed successfully.")
    await handler.clear()

    handler.event_times.end_time = get_now()


async def post_fill_tasks(
    handler: LN2Handler,
    notifier: Notifier | None = None,
    write_data: bool = False,
    data_path: str | pathlib.Path | None = None,
    data_extra_time: float | None = None,
    api_data_route: str = "http://lvm-hub.lco.cl:8090/api/spectrographs/fills/measurements",
    generate_data_plots: bool = True,
) -> dict[str, pathlib.Path]:
    """Runs the post-fill tasks.

    Parameters
    ----------
    handler
        The `.LN2Handler` instance.
    write_data
        Whether to collect fill metrology data and write it to disk.
    data_path
        The path where to write the data. If `None` writes it to the current directory.
    data_extra_time
        Extra time to wait after the fill before collecting data.
    api_data_route
        The API route to retrive the fill data.
    generate_data_plots
        Whether to generate plots from the data.

    Returns
    -------
    plot_paths
        A mapping of plot type to path.

    """

    log = handler.log
    log.info("Running post-fill tasks.")

    event_times = handler.event_times
    plot_paths: dict[str, pathlib.Path] = {}

    if (
        not write_data
        or not event_times.start_time
        or not event_times.end_time
        or not api_data_route
    ):
        write_data = False
        log.debug("Skipping data collection.")

    if event_times.start_time and event_times.end_time:
        interval = (event_times.end_time - event_times.start_time).total_seconds()
        if interval < 120:
            log.warning("Fill duration was less than 2 minutes. Not collecting data.")
            write_data = False

    if write_data and event_times.start_time and event_times.end_time:
        if data_extra_time:
            if handler.aborted or handler.failed:
                log.warning(
                    "Fill was aborted or failed. Not waiting "
                    "additional time to collect data."
                )
            else:
                log.info(f"Waiting {data_extra_time} seconds before collecting data.")

                if notifier is not None:
                    await notifier.post_to_slack(
                        f"Fill notifications will be delayed {data_extra_time:.0f} "
                        "seconds while collecting post-fill data."
                    )

                await asyncio.sleep(data_extra_time)

        if data_path is None:
            data_path = pathlib.Path.cwd() / "fill_data.parquet"
        else:
            data_path = pathlib.Path(data_path)

        try:
            log.info("Retrieving and writing measurements.")

            end_time = event_times.end_time + timedelta(seconds=data_extra_time or 0.0)
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.get(
                    api_data_route,
                    params={
                        "start_time": int(event_times.start_time.timestamp()),
                        "end_time": int(end_time.timestamp()),
                    },
                )
                response.raise_for_status()

                data = (
                    polars.DataFrame(response.json())
                    .with_columns(polars.col.time.cast(polars.Datetime("ms")))
                    .sort("time")
                    .drop_nulls()
                )

                data_path.parent.mkdir(parents=True, exist_ok=True)
                data.write_parquet(data_path)
                log.debug(f"Fill data written to {data_path!s}")

                if generate_data_plots:
                    log.debug("Generating plots.")
                    plot_path_root = str(data_path.with_suffix(""))
                    plot_paths = await run_in_executor(
                        generate_plots,
                        data,
                        plot_path_root,
                    )
                    plot_paths_transparent = await run_in_executor(
                        generate_plots,
                        data,
                        plot_path_root,
                        transparent=True,
                    )
                    plot_paths.update(plot_paths_transparent)
                    log.debug(f"Plots saved to {plot_path_root}*.")

        except Exception as ee:
            log.error(f"Failed to retrieve fill data from API: {ee!r}")

    return plot_paths


def generate_plots(
    data: polars.DataFrame,
    plot_path_root: str,
    transparent: bool = False,
    include_ccd_tempratures: bool = False,
):
    """Generates measurement plots.

    Parameters
    ----------
    data
        The data to plot.
    plot_path_root
        The root path where to save the plots. A number of plots will be generated
        using this root path.
    transparent
        Whether to save the plots with a transparent background. The colours will
        be adjusted to be visible on a dark background. The paths will be suffixed
        with ``_transparent``. No PDFs will be generated in this case.
    include_ccd_tempratures
        Whether to include CCD temperatures in the temperature plot.

    Returns
    -------
    paths
        A mapping of plot type to path.

    """

    import matplotlib.pyplot as plt

    paths: dict[str, pathlib.Path] = {}

    colours: dict[str, str] = {
        "r": "red",
        "b": "cyan" if transparent else "blue",
        "z": "magenta",
    }
    linestyles: dict[str, str] = {"1": "-", "2": "--", "3": "-."}

    transparent_suffix = "_transparent" if transparent else ""

    date = data[0, "time"].strftime("%Y-%m-%d")

    with plt.ioff():
        if transparent:
            plt.style.use("dark_background")
        else:
            plt.style.use("seaborn-v0_8-whitegrid")
            plt.rcParams["axes.facecolor"] = "white"
            plt.rcParams["figure.facecolor"] = "white"
            plt.rcParams["savefig.facecolor"] = "white"

        # Pressures
        fig, ax = plt.subplots(figsize=(12, 8))

        pressures = data.select(
            polars.col.time,
            polars.selectors.starts_with("pressure_"),
        )

        for spec, camera in itertools.product("123", "brz"):
            column = f"pressure_{camera}{spec}"
            if column not in pressures.columns:
                continue

            cam_pressure = pressures[column].to_numpy()

            colour = colours.get(camera, "w" if transparent else "k")
            linestyle = linestyles.get(spec, "-")

            label = f"{camera}{spec}"

            ax.plot(
                pressures["time"].to_numpy(),
                cam_pressure,
                label=label,
                color=colour,
                linestyle=linestyle,
            )

        ax.set_title(f"Pressure during fill — {date}")
        ax.set_xlabel("Time")
        ax.set_ylabel("Pressure [torr]")

        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), prop={"size": 9})

        plt.ticklabel_format(style="sci", axis="y", scilimits=(0, 0))

        if not transparent:
            path = f"{plot_path_root}_pressure{transparent_suffix}.pdf"
            fig.savefig(path)
            paths[f"pressure{transparent_suffix}_pdf"] = pathlib.Path(path)

        path = f"{plot_path_root}_pressure{transparent_suffix}.png"
        fig.savefig(path, dpi=300, transparent=transparent)
        paths[f"pressure{transparent_suffix}_png"] = pathlib.Path(path)

        plt.close(fig)

        # Temperatures
        fig, ax = plt.subplots(figsize=(12, 8))

        temps = data.select(
            polars.col.time,
            polars.selectors.starts_with("temp_"),
        )

        for spec, camera, sensor in itertools.product("123", "brz", ["ln2", "ccd"]):
            if sensor == "ccd" and not include_ccd_tempratures:
                continue

            column = f"temp_{camera}{spec}_{sensor}"
            if column not in temps.columns:
                continue

            cam_temp = temps[column].to_numpy()

            colour = colours.get(camera, "w" if transparent else "k")
            linestyle = linestyles.get(spec, "-")
            linewidth = 1.5 if sensor == "ln2" else 1

            label = f"{camera}{spec} ({sensor.upper()})"

            ax.plot(
                temps["time"].to_numpy(),
                cam_temp,
                label=label,
                color=colour,
                linestyle=linestyle,
                linewidth=linewidth,
            )

        ax.set_title(f"Temperature during fill — {date}")
        ax.set_xlabel("Time")
        ax.set_ylabel("Temperature [C]")

        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), prop={"size": 9})

        if not transparent:
            path = f"{plot_path_root}_temps{transparent_suffix}.pdf"
            fig.savefig(path)
            paths[f"temps{transparent_suffix}_pdf"] = pathlib.Path(path)

        path = f"{plot_path_root}_temps{transparent_suffix}.png"
        fig.savefig(path, dpi=300, transparent=transparent)
        paths[f"temps{transparent_suffix}_png"] = pathlib.Path(path)

        plt.close(fig)

        # Thermistors
        fig, ax = plt.subplots(figsize=(12, 8))

        therms = data.select(
            polars.col.time,
            polars.selectors.starts_with("thermistor_"),
        )

        cameras = ["".join(item)[::-1] for item in itertools.product("123", "brz")]
        for channel in ["supply"] + cameras:
            column = f"thermistor_{channel}"
            if column not in therms.columns:
                continue

            cam_therm = therms[column].to_numpy()

            if len(channel) == 2:
                camera, spec = channel
                colour = colours.get(camera, "w" if transparent else "k")
                linestyle = linestyles.get(spec, "-")
            else:
                colour = "g" if transparent else "k"
                linestyle = "-"

            ax.plot(
                therms["time"].to_numpy(),
                cam_therm,
                label=channel,
                color=colour,
                linestyle=linestyle,
            )

        ax.set_title(f"Thermistors during fill — {date}")
        ax.set_xlabel("Time")
        ax.set_ylabel("State")

        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), prop={"size": 9})

        if not transparent:
            path = f"{plot_path_root}_thermistors{transparent_suffix}.pdf"
            fig.savefig(path)
            paths[f"thermistors{transparent_suffix}_pdf"] = pathlib.Path(path)

        path = f"{plot_path_root}_thermistors{transparent_suffix}.png"
        fig.savefig(path, dpi=300, transparent=transparent)
        paths[f"thermistors{transparent_suffix}_png"] = pathlib.Path(path)

        plt.close(fig)

    return paths
