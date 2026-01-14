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
from logging import FileHandler
from tempfile import NamedTemporaryFile

from typing import TYPE_CHECKING, Literal

import httpx
import polars
from rich.prompt import Confirm

from sdsstools.logger import get_logger
from sdsstools.utils import run_in_executor

from lvmcryo import __version__
from lvmcryo.config import (
    Actions,
    Config,
    InteractiveMode,
    NotificationLevel,
    ParameterOrigin,
)
from lvmcryo.handlers import LN2Handler, close_all_valves
from lvmcryo.handlers.ln2 import get_now
from lvmcryo.notifier import Notifier
from lvmcryo.tools import (
    DBHandler,
    LockExistsError,
    add_json_handler,
    ensure_lock,
    register_parameter_origin,
)
from lvmcryo.validate import validate_fill


if TYPE_CHECKING:
    from sdsstools.logger import SDSSLogger

__all__ = ["fill_runner", "ln2_runner"]


async def signal_handler(handler: LN2Handler, log: logging.Logger):
    """Handles signals to close all valves and exit cleanly."""

    handler.failed = True
    handler.aborted = True

    log.error("User aborted the process. Closing all valves before exiting.")
    try:
        await handler.stop(only_active=False)
    except Exception as err:
        log.error(f"Failed closing LN2 valves: {err!r}")

    await handler.clear()

    handler.event_times.fail_time = get_now()
    handler.event_times.abort_time = get_now()
    handler.event_times.end_time = get_now()

    log.error("Exiting now. No data or notifications will be sent.")

    # Prevent the event loop from throwing some exception when we exit.
    asyncio.get_running_loop().stop()

    sys.exit(1)


class LN2RunnerError(Exception):
    """An error occurred during the LN2 runner execution."""

    def __init__(self, *args, propagate: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.propagate = propagate


@register_parameter_origin
async def ln2_runner(
    action: Literal["purge", "fill", "purge-and-fill"] = "purge-and-fill",
    cameras: list[str] | None = None,
    profile: str | None = None,
    config_file: pathlib.Path | None = None,
    dry_run: bool = False,
    interactive: Literal["auto", True, False, "yes", "no"] = False,
    no_prompt: bool = True,
    with_traceback: bool = False,
    clear_lock: bool = False,
    use_thermistors: bool = True,
    require_all_thermistors: bool = False,
    check_pressures: bool = True,
    check_temperatures: bool = True,
    check_o2_sensors: bool = True,
    max_pressure: float | None = None,
    max_temperature: float | None = None,
    purge_time: float | None = None,
    min_purge_time: float | None = None,
    max_purge_time: float | None = None,
    fill_time: float | None = None,
    min_fill_time: float | None = None,
    max_fill_time: float | None = None,
    notify: bool = False,
    slack: bool = True,
    email: bool = True,
    email_level: Literal["error", "info"] = "error",
    quiet: bool = False,
    verbose: bool = False,
    write_log: bool = False,
    log_path: pathlib.Path | None = None,
    write_json: bool = True,
    write_data: bool = False,
    data_path: pathlib.Path | None = None,
    data_extra_time: float = 60,
    log: SDSSLogger | None = None,
    __parameter_origin: dict[str, ParameterOrigin | None] = {},
):
    """Runs LN2 purge/fill/abort/clear actions.

    This is a wrapper allowing the CLI functionality to be called programmatically.

    Parameters
    ----------
    action
        The action to perform. One of ``purge``, ``fill``, or ``purge-and-fill``.
    cameras
        The cameras to fill. Defaults to all cameras. Ignored for ``purge``, ``abort``
        and ``clear_lock``.
    profile
        The profile to use.
    config_file
        The configuration file. Defaults to the internal configuration file.
    dry_run
        Test run the code but do not actually open/close any valves.
    interactive
        Controls whether the interactive features are shown.
        When ``auto`` (the default), interactivity is determined based
        on the console mode.
    no_prompt
        Does not prompt the user to finish or abort a purge/fill.
        Prompting is required if ``fill_time`` or ``purge_time`` are not provided
        and thermistors are not used.
    with_traceback
        Show the full traceback in case of an error. If not set, only the error message
        is shown. The full traceback is always logged to the log file.
    clear_lock
        Clears the lock file if it exists before carrying out the action.
    use_thermistors
        Use thermistor values to determine purge/fill time.
    require_all_thermistors
        If set, waits until all thermistors have activated before closing any of the
        valves. This prevents over-pressures in the cryostats when only some
        of the valves are open. Ignored if ``use_thermistors=False``.
    check_pressures
        Aborts purge/fill if the pressure of any cryostat is above the limit.
    check_temperatures
        Aborts purge/fill if the temperature of a fill cryostat is above the limit.
    check_o2_sensors
        Aborts purge/fill if the oxygen sensor reads below the limit.
    max_pressure
        Maximum cryostat pressure if ``--check-pressure``. Defaults to internal value.
    max_temperature
        Maximum cryostat temperature if ``check_temperature=True``.
        Defaults to internal value.
    purge_time
        Purge time in seconds. If ``use_thermistors=True``, this is used
        as the maximum purge time.
    min_purge_time
        Minimum purge time in seconds. Defaults to internal value.
    max_purge_time
        Maximum purge time in seconds. Defaults to internal value.
    fill_time
        Fill time in seconds. If ``use_thermistors=True``, this is used as
        the maximum fill time.
    min_fill_time
        Minimum fill time in seconds. Defaults to internal value.
    max_fill_time
        Maximum fill time in seconds. Defaults to internal value.
    notify
        Sends a notification when the action is completed. In not set,
        ``slack`` and ``email`` are ignored. Notifications are not sent
        for the ``abort`` and ``clear-lock`` actions.
    slack
        Send a Slack notification when the action is completed.
    email
        Send an email notification when the action is completed.
    email_level
        Send an email notification only on error or always.
    quiet
        Disable logging to stdout.
    verbose
        Outputs additional information to stdout.
    write_log
        Saves the log to a file.
    log_path
        Path where to save the log file. Implies ``write_log=True``.
        Defaults to the current directory.
    write_json
        Saves the log in JSON format. Uses the same path as the file log. Ignored if
        ``write_log`` is not passed.
    write_data
        Saves cryostat pressures and temperatures taken during purge/fill to a
        parquet file.
    data_path
        Path where to save the data. Implies ``write_data=True``.
        Defaults to a path relative to the log path.
    data_extra_time
        Additional time to take cryostat data after the action has been completed.
        The command will not complete until data collection has finished.
    log
        The logger instance to use. If `None`, a new logger is created.
    __parameter_origin
        Used internally to track parameter origins. Do not pass this parameter
        manually.

    """

    try:
        action_enum = Actions(action)

        if interactive is True:
            interactive = "yes"
        elif interactive is False:
            interactive = "no"
        interactive_enum = InteractiveMode(interactive)

        email_level_enum = NotificationLevel(email_level)

        config = Config(
            action=action_enum,
            cameras=cameras or [],
            config_file=config_file,
            dry_run=dry_run,
            clear_lock=clear_lock,
            with_traceback=with_traceback,
            interactive=interactive_enum,
            no_prompt=no_prompt,
            notify=notify,
            slack=slack,
            email=email,
            email_level=email_level_enum,
            use_thermistors=use_thermistors,
            require_all_thermistors=require_all_thermistors,
            check_pressures=check_pressures,
            check_temperatures=check_temperatures,
            check_o2_sensors=check_o2_sensors,
            max_pressure=max_pressure,
            max_temperature=max_temperature,
            purge_time=purge_time,
            min_purge_time=min_purge_time,
            max_purge_time=max_purge_time,
            fill_time=fill_time,
            min_fill_time=min_fill_time,
            max_fill_time=max_fill_time,
            quiet=quiet,
            verbose=verbose,
            write_log=write_log,
            write_json=write_json,
            log_path=log_path,
            write_data=write_data,
            data_path=data_path,
            data_extra_time=data_extra_time,
            version=__version__,
            profile=profile,
            param_origin=__parameter_origin,
        )
        internal_config = config.internal_config
    except Exception as err:
        raise LN2RunnerError(f"Error parsing configuration: {err!r}") from err

    # Create log here and use its console for stdout.
    if log is None:
        log = get_logger("lvmcryo", use_rich_handler=True)
        log.setLevel(5)

    stdout_console = log.rich_console
    assert stdout_console is not None

    error: Exception | None = None
    skip_finally: bool = False

    json_path: pathlib.Path | None = None
    json_handler: FileHandler | None = None
    images: dict[str, pathlib.Path | None] = {}

    if config.write_log and config.log_path:
        log.start_file_logger(str(config.log_path))

        if config.write_json:
            json_path = config.log_path.with_suffix(".json")
            json_handler = add_json_handler(log, json_path)

    else:
        # We're still creating log files, but to a temporary location. This is
        # just to be able to send the log body in a notification email and include
        # it when loading the DB.
        temp_file = NamedTemporaryFile()
        log.start_file_logger(temp_file.name)
        json_path = pathlib.Path(temp_file.name).with_suffix(".json")
        json_handler = add_json_handler(log, json_path)

    if verbose:
        log.sh.setLevel(5)
    if quiet:
        log.sh.setLevel(30)

    # Always create a notifier. If the element is disabled the
    # notifier won't do anything. This simplifies the code.
    notifier = Notifier(internal_config)
    notifier.disabled = not config.notify
    notifier.slack_disabled = not config.slack
    notifier.email_disabled = not config.email

    if not config.notify:
        log.debug("Notifications are disabled and will not be emitted.")

    if config.config_file is not None:
        log.info(f"Using configuration file: {config.config_file!s}")

    if not config.no_prompt:
        stdout_console.print(f"Action {config.action.value} will run with:")
        stdout_console.print(config.model_dump())
        if not Confirm.ask(
            "Continue with this configuration?",
            default=False,
            show_default=True,
            console=stdout_console,
        ):
            return False

    # Create the LN2 handler.
    try:
        handler = LN2Handler(
            config.cameras,
            interactive=config.interactive == "yes",
            log=log,
            valve_info=config.valve_info,
            dry_run=config.dry_run,
            alerts_route=internal_config["api_routes"]["alerts"],
            check_o2_sensors=config.check_o2_sensors,
        )
    except Exception as err:
        raise LN2RunnerError(
            f"Error creating LN2 handler: {err!r}",
            propagate=config.with_traceback,
        ) from err

    lockfile_path = pathlib.Path(internal_config.get("lockfile", "/data/lvmcryo.lock"))

    if lockfile_path.exists() and config.clear_lock:
        log.warning("Lock file exists. Removing it because --clear-lock.")
        lockfile_path.unlink()

    try:
        db_handler = DBHandler(action, handler, config, json_handler=json_handler)
        record_pk = await db_handler.write(complete=False)
        if record_pk:
            log.debug(f"Record {record_pk} created in the database.")
    except Exception as err:
        raise LN2RunnerError(
            f"Error creating database record: {err!r}",
            propagate=config.with_traceback,
        ) from err

    try:
        async with ensure_lock(
            lockfile_path,
            monitor=True,
            log=log,
            on_release_callback=handler.abort(raise_error=False, close_valves=True),
        ):
            # Calculate the expected maximum run time.
            max_time: float = 2 * 3600  # It should never take longer than two hours.
            if config.max_purge_time is not None and config.max_fill_time is not None:
                max_time = config.max_purge_time + config.max_fill_time + 300.0

            # Run worker.
            await asyncio.wait_for(
                fill_runner(
                    handler,
                    config,
                    notifier,
                    db_handler=db_handler,
                ),
                timeout=max_time,
            )

            # Check handler status.
            if handler.failed:
                raise RuntimeError(
                    "No exceptions were raised but the LN2 handler reports a failure."
                )

    except LockExistsError:
        if config.notify:
            log.warning("Sending failure notifications.")
            await notifier.notify_after_fill(
                False,
                error_message=f"LN2 {config.action.value} failed because a lockfile "
                "was already present.",
            )

        # Do not do anything special for this error, just exit.
        skip_finally = True

        raise

    except Exception as err:
        # Log the traceback to file but do not print.
        orig_sh_level = log.sh.level
        log.sh.setLevel(1000)

        log.exception(f"Error during {config.action.value}: {err!s}", exc_info=err)
        if isinstance(err, asyncio.TimeoutError):
            log.error("One or more operations timed out.")

        log.sh.setLevel(orig_sh_level)

        # Fail the action.
        handler.failed = True
        skip_finally = True

        error = err
        raise LN2RunnerError(str(err), propagate=config.with_traceback) from err

    else:
        log.info(f"LN2 {config.action.value} completed successfully.")

    finally:
        # At this point all the valves are closed so we can remove the signal handlers.
        for signame in ("SIGINT", "SIGTERM"):
            asyncio.get_running_loop().remove_signal_handler(getattr(signal, signame))

        handler.event_times.end_time = get_now()
        await handler.clear()

        log.info(f"Event times:\n{handler.event_times.model_dump_json(indent=2)}")

        # Make sure all valves are closed.
        try:
            log.info("Ensuring all valves are closed.")
            await asyncio.wait_for(
                handler.stop(only_active=False, close_valves=True),
                timeout=30,
            )
        except Exception as err:
            log.error(f"Error closing valves before exiting: {err}")

        if not skip_finally:
            # Do a quick update of the DB record since post_fill_tasks() may
            # block for a long time.
            await db_handler.write(error=error)

            plot_paths = await post_fill_tasks(
                handler,
                notifier=notifier,
                write_data=config.write_data,
                data_path=config.data_path,
                data_extra_time=config.data_extra_time if error is None else None,
                api_data_route=internal_config["api_routes"]["fill_data"],
            )

            if (
                config.write_data
                and config.data_path
                and config.data_path.exists()
                and not error
            ):
                validate_failed, validate_error = validate_fill(
                    handler,
                    config,
                    log=log,
                )
                if validate_failed and error is None:
                    await notifier.post_to_slack(
                        "Fill validation failed. Check the log for details.",
                        level=NotificationLevel.error,
                    )
                    handler.failed = True
                    error = RuntimeError(validate_error)
                elif not validate_failed:
                    log.info("Fill validation completed successfully.")

            log.info("Writing fill metadata to database.")
            await db_handler.write(complete=True, plot_paths=plot_paths, error=error)

            if config.notify:
                images = {
                    "pressure": plot_paths.get("pressure_png", None),
                    "temps": plot_paths.get("temps_png", None),
                    "thermistors": plot_paths.get("thermistors_png", None),
                }

                if error:
                    log.warning("Sending failure notifications.")
                    await notifier.notify_after_fill(
                        False,
                        error_message=error,
                        handler=handler,
                        images=images,
                        record_pk=record_pk,
                    )

                elif config.email_level == NotificationLevel.info:
                    # The handler has already emitted a notification to
                    # Slack so just send an email.

                    # TODO: include log and more data here.
                    # For now it's just plain text.

                    log.info("Sending notification email.")
                    await notifier.notify_after_fill(
                        True,
                        handler=handler,
                        images=images,
                        post_to_slack=False,  # Already done.
                        record_pk=record_pk,
                    )

            if error:
                raise error


async def fill_runner(
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
        check_o2_sensors=config.check_o2_sensors,
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
