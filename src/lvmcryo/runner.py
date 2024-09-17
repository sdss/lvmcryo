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
from datetime import datetime

from typing import TYPE_CHECKING, Any

import httpx
import polars

from sdsstools.utils import run_in_executor

from lvmcryo.config import Actions
from lvmcryo.handlers import LN2Handler, close_all_valves
from lvmcryo.notifier import Notifier


if TYPE_CHECKING:
    from lvmcryo.config import Config


__all__ = ["ln2_runner"]


async def signal_handler(handler: LN2Handler, log: logging.Logger):
    """Handles signals to close all valves and exit cleanly."""

    log.error("User aborted the process. Closing all valves before exiting.")
    await handler.close_valves(only_active=False)
    await handler.clear()

    handler.aborted = True
    handler.event_times.aborted = handler._get_now()

    handler.event_times.failed = handler._get_now()
    handler.event_times.aborted = handler._get_now()


async def ln2_runner(
    handler: LN2Handler,
    config: Config,
    notifier: Notifier | None = None,
):
    """Runs the purge/fill process.

    This function is usually called by the CLI.

    """

    if notifier is None:
        # Create a notifier but immediately disable it.
        notifier = Notifier()
        notifier.disabled = True

    log = handler.log

    # Record options used.
    config_json = config.model_dump_json(indent=2)
    log.debug(f"Running {config.action.value} with configuration:\n{config_json}")

    if config.dry_run:
        log.warning("Running in dry-run mode. No valves will be operated.")

    # Inform of the start of the fill in Slack.
    now = handler._get_now()
    now_str = now.strftime("%H:%M:%S")

    action = config.action.value

    if config.use_thermistors or config.purge_time is None or config.fill_time is None:
        await notifier.post_to_slack(f"Starting LN₂ `{action}` at {now_str}.")
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
        await handler.purge(
            use_thermistor=config.use_thermistors,
            min_purge_time=config.min_purge_time,
            max_purge_time=max_purge_time,
            prompt=not config.no_prompt,
        )

        if handler.failed or handler.aborted:
            await handler.clear()
            raise RuntimeError("Purge failed or was aborted.")

    if config.action == Actions.purge_fill or config.action == Actions.fill:
        await notifier.post_to_slack("Starting fill.")
        max_fill_time = config.fill_time or config.max_fill_time
        await handler.fill(
            use_thermistors=config.use_thermistors,
            min_fill_time=config.min_fill_time,
            max_fill_time=max_fill_time,
            prompt=not config.no_prompt,
        )

        if handler.failed or handler.aborted:
            await handler.clear()
            raise RuntimeError("Fill failed or was aborted.")

    await notifier.post_to_slack(f"LN₂ `{action}` completed successfully.")
    await handler.clear()


async def post_fill_tasks(
    handler: LN2Handler,
    write_data: bool = False,
    data_path: str | pathlib.Path | None = None,
    data_extra_time: float | None = None,
    api_data_route: str = "http://lvm-hub.lco.cl:8080/api/spectrographs/fills/measurements",
    generate_data_plots: bool = True,
    write_to_db: bool = False,
    api_db_route: str = "http://lvm-hub.lco.cl:8080/api/spectrographs/fills/register",
    db_extra_payload: dict[str, Any] = {},
) -> int | None:
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
    write_to_db
        Whether to write the data to the database.
    api_db_route
        The API route to write the data to the database.
    db_extra_payload
        Extra payload to send to the database registration endpoint.

    Returns
    -------
    record_id
        The primary key of the associated record if the fill data was written to
        the database. `None` otherwise.

    """

    log = handler.log
    log.info("Running post-fill tasks.")

    record_id: int | None = None

    start_time: datetime | None = None
    end_time: datetime | None = None

    event_times = handler.event_times

    if not event_times.purge_start and not event_times.fill_start:
        # Nothing to do, the fill never happened. Probably failed or was aborted.
        pass
    else:
        if event_times.purge_start:
            start_time = event_times.purge_start
        elif event_times.fill_start:
            start_time = event_times.fill_start

    if event_times.failed or event_times.aborted:
        end_time = event_times.failed or event_times.aborted
    else:
        end_time = event_times.fill_complete or event_times.purge_complete

    if not write_data or not start_time or not end_time or not api_data_route:
        write_data = False
        log.debug("Skipping data collection.")

    if write_data and start_time and end_time:
        if data_extra_time:
            log.info(f"Waiting {data_extra_time} seconds before collecting data.")
            await asyncio.sleep(data_extra_time)

        if data_path is None:
            data_path = pathlib.Path.cwd() / "fill_data.parquet"
        else:
            data_path = pathlib.Path(data_path)

        try:
            log.debug("Retrieving and writing measurements.")
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.get(
                    api_data_route,
                    params={
                        "start_time": int(start_time.timestamp()),
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

                if generate_data_plots:
                    log.debug("Generating plots.")
                    plot_path_root = str(data_path.with_suffix(""))
                    await run_in_executor(generate_plots, data, plot_path_root)
                    await run_in_executor(
                        generate_plots,
                        data,
                        plot_path_root,
                        transparent=True,
                    )

        except Exception as ee:
            log.error(f"Failed to retrieve fill data from API: {ee!r}")

        else:
            log.debug(f"Fill data written to {data_path}.")

    if write_to_db and api_db_route:
        try:
            log.debug("Writing fill data to database.")
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.post(
                    api_db_route,
                    json={
                        "start_time": date_json(start_time),
                        "end_time": date_json(end_time),
                        "purge_start": date_json(event_times.purge_start),
                        "purge_complete": date_json(event_times.purge_complete),
                        "fill_start": date_json(event_times.fill_start),
                        "fill_complete": date_json(event_times.fill_complete),
                        "fail_time": date_json(event_times.failed),
                        "abort_time": date_json(event_times.aborted),
                        "failed": handler.failed,
                        "aborted": handler.aborted,
                        **db_extra_payload,
                    },
                )
                response.raise_for_status()

                record_id = response.json()

        except Exception as ee:
            log.error(f"Failed to write fill data to database: {ee!r}")

    return record_id


def date_json(date: datetime | None) -> str | None:
    """Serialises a datetime object to a JSON string."""

    return date.isoformat() if date else None


def generate_plots(
    data: polars.DataFrame,
    plot_path_root: str,
    transparent: bool = False,
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

    Returns
    -------
    paths
        A list of generated plot paths.

    """

    import matplotlib.pyplot as plt

    paths: list[pathlib.Path] = []

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
            paths.append(pathlib.Path(path))

        path = f"{plot_path_root}_pressure{transparent_suffix}.png"
        fig.savefig(path, dpi=300, transparent=transparent)
        paths.append(pathlib.Path(path))

        plt.close(fig)

        # Temperatures
        fig, ax = plt.subplots(figsize=(12, 8))

        temps = data.select(
            polars.col.time,
            polars.selectors.starts_with("temp_"),
        )

        for spec, camera, sensor in itertools.product("123", "brz", ["ln2", "ccd"]):
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
            paths.append(pathlib.Path(path))

        path = f"{plot_path_root}_temps{transparent_suffix}.png"
        fig.savefig(path, dpi=300, transparent=transparent)
        paths.append(pathlib.Path(path))

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
                colour = "w" if transparent else "k"
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
            paths.append(pathlib.Path(path))

        path = f"{plot_path_root}_thermistors{transparent_suffix}.png"
        fig.savefig(path, dpi=300, transparent=transparent)
        paths.append(pathlib.Path(path))

        plt.close(fig)
