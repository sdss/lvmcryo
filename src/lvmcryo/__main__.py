#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import pathlib
import signal
import warnings
from functools import wraps
from tempfile import NamedTemporaryFile

from typing import Annotated, Optional

import typer
from rich.console import Console
from typer import Argument, Option
from typer.core import TyperGroup

from lvmcryo.config import Actions, Config, InteractiveMode, NotificationLevel


LOCKFILE = pathlib.Path("/data/lvmcryo.lock")

info_console = Console()
err_console = Console(stderr=True)


def cli_coro(
    signals=(signal.SIGHUP, signal.SIGTERM, signal.SIGINT),
    shutdown_func=None,
):
    """Decorator function that allows defining coroutines with click."""

    def decorator_cli_coro(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            loop = asyncio.get_event_loop()
            if shutdown_func:
                for ss in signals:
                    loop.add_signal_handler(ss, shutdown_func, ss, loop)
            return loop.run_until_complete(f(*args, **kwargs))

        return wrapper

    return decorator_cli_coro


class NaturalOrderGroup(TyperGroup):
    """A Typer group that lists commands in order of definition."""

    def list_commands(self, ctx):
        return self.commands.keys()


cli = typer.Typer(
    cls=NaturalOrderGroup,
    rich_markup_mode="rich",
    context_settings={"obj": {}},
    no_args_is_help=True,
    help="CLI for LVM cryostats.",
)


@cli.command("ln2")
@cli_coro()
async def ln2(
    #
    # Arguments
    #
    action: Annotated[
        Actions,
        Argument(help="Action to perform."),
    ] = Actions.purge_fill,
    cameras: Annotated[
        list[str] | None,
        Argument(
            help="Comma-separated cameras to fill. Defaults to all cameras. "
            "Ignored for [green]purge[/], [green]abort[/] and [green]clear[/].",
        ),
    ] = None,
    #
    # General options
    #
    config_file: Annotated[
        Optional[pathlib.Path],
        Option(
            dir_okay=False,
            exists=True,
            envvar="LVMCRYO_CONFIG_FILE",
            help="The configuration file to use to set the default values. "
            "Defaults to the internal configuration file.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        Option(
            "--dry-run",
            help="Test run the code but do not actually open/close any valves.",
        ),
    ] = False,
    interactive: Annotated[
        InteractiveMode,
        Option(
            "--interactive",
            "-i",
            envvar="LVMCRYO_INTERACTIVE",
            help="Controls whether the interactive features are shown. When "
            "--interactive auto (the default), interactivity is determined "
            "based on console mode.",
        ),
    ] = InteractiveMode.auto,
    no_prompt: Annotated[
        bool,
        Option(
            "--no-prompt",
            "-y",
            envvar="LVMCRYO_NO_PROMPT",
            help="Does not prompt the user to finish or abort a purge/fill. "
            "Prompting is required if --fill-time or --purge-time are not provided "
            "and thermistors are not used.",
        ),
    ] = False,
    with_traceback: Annotated[
        bool,
        Option(
            "--with-traceback",
            envvar="LVMCRYO_DEBUG",
            help="Show the full traceback in case of an error. If not set, only "
            "the error message is shown. The full traceback is always logged "
            "to the log file.",
            show_default=False,
        ),
    ] = False,
    #
    # Purge and fill options
    #
    use_thermistors: Annotated[
        bool,
        Option(
            " /--no-use-thermistors",
            help="Use thermistor values to determine purge/fill time.",
            rich_help_panel="Purge and fill options",
        ),
    ] = True,
    check_pressures: Annotated[
        bool,
        Option(
            " /--no-check-pressures",
            help="Aborts purge/fill if the pressure of any cryostat "
            "is above the limit.",
            rich_help_panel="Purge and fill options",
        ),
    ] = True,
    check_temperatures: Annotated[
        bool,
        Option(
            " /--no-check-temperatures",
            help="Aborts purge/fill if the temperature of a fill cryostat "
            "is above the limit.",
            rich_help_panel="Purge and fill options",
        ),
    ] = True,
    max_pressure: Annotated[
        Optional[float],
        Option(
            help="Maximum cryostat pressure if --check-pressure. "
            "Defaults to internal value.",
            show_default=False,
            rich_help_panel="Purge and fill options",
        ),
    ] = None,
    max_temperature: Annotated[
        Optional[float],
        Option(
            help="Maximum cryostat temperature if --check-temperature. "
            "Defaults to internal value.",
            show_default=False,
            rich_help_panel="Purge and fill options",
        ),
    ] = None,
    purge_time: Annotated[
        Optional[float],
        Option(
            help="Purge time in seconds. If --use-thermistors, this is "
            "used as the maximum purge time.",
            rich_help_panel="Purge and fill options",
        ),
    ] = None,
    min_purge_time: Annotated[
        Optional[float],
        Option(
            help="Minimum purge time in seconds. Defaults to internal value.",
            show_default=False,
            rich_help_panel="Purge and fill options",
        ),
    ] = None,
    max_purge_time: Annotated[
        Optional[float],
        Option(
            help="Maximum purge time in seconds. Defaults to internal value.",
            show_default=False,
            rich_help_panel="Purge and fill options",
        ),
    ] = None,
    fill_time: Annotated[
        Optional[float],
        Option(
            help="Fill time in seconds. If --use-thermistors, this is "
            "used as the maximum fill time.",
            rich_help_panel="Purge and fill options",
        ),
    ] = None,
    min_fill_time: Annotated[
        Optional[float],
        Option(
            help="Minimum fill time in seconds. Defaults to internal value.",
            show_default=False,
            rich_help_panel="Purge and fill options",
        ),
    ] = None,
    max_fill_time: Annotated[
        Optional[float],
        Option(
            help="Maximum fill time in seconds. Defaults to internal value.",
            show_default=False,
            rich_help_panel="Purge and fill options",
        ),
    ] = None,
    #
    # Notification options
    #
    notify: Annotated[
        bool,
        Option(
            "--notify",
            envvar="LVMCRYO_NOTIFY",
            help="Sends a notification when the action is completed. "
            "In not set, --slack and --email are ignored. "
            "Notifications are not sent for the [green]abort[/] "
            "and [green]clear-lock[/] actions.",
            rich_help_panel="Notifications",
            show_default=True,
        ),
    ] = False,
    slack: Annotated[
        bool,
        Option(
            help="Send a Slack notification when the action is completed.",
            rich_help_panel="Notifications",
        ),
    ] = True,
    email: Annotated[
        bool,
        Option(
            help="Send an email notification when the action is completed.",
            rich_help_panel="Notifications",
        ),
    ] = True,
    email_level: Annotated[
        NotificationLevel,
        Option(
            envvar="LVMCRYO_EMAIL_LEVEL",
            help="Send an email notification only on error or always.",
            case_sensitive=False,
            rich_help_panel="Notifications",
        ),
    ] = NotificationLevel.error,
    #
    # Logging options
    #
    quiet: Annotated[
        bool,
        Option(
            "--quiet",
            "-q",
            help="Disable logging to stdout.",
            rich_help_panel="Logging",
        ),
    ] = False,
    verbose: Annotated[
        bool,
        Option(
            "--verbose",
            "-v",
            help="Outputs additional information to stdout.",
            rich_help_panel="Logging",
        ),
    ] = False,
    write_log: Annotated[
        bool,
        Option(
            "--write-log",
            "-S",
            envvar="LVMCRYO_WRITE_LOG",
            help="Saves the log to a file.",
            rich_help_panel="Logging",
        ),
    ] = False,
    log_path: Annotated[
        Optional[pathlib.Path],
        Option(
            exists=False,
            help="Path where to save the log file. Implies --write-log. "
            "Defaults to the current directory.",
            rich_help_panel="Logging",
        ),
    ] = None,
    write_json: Annotated[
        bool,
        Option(
            "--write-json",
            envvar="LVMCRYO_WRITE_JSON",
            help="Saves the log in JSON format. Uses the same path as the file log."
            "Ignored if --write-log is not passed.",
            rich_help_panel="Logging",
        ),
    ] = True,
    write_data: Annotated[
        bool,
        Option(
            "--write-data",
            envvar="LVMCRYO_WRITE_DATA",
            help="Saves cryostat pressures and temperatures taken during purge/fill "
            "to a parquet file.",
            rich_help_panel="Logging",
        ),
    ] = False,
    data_path: Annotated[
        Optional[pathlib.Path],
        Option(
            exists=False,
            help="Path where to save the data. Implies --write-data. "
            "Defaults to a path relative to the log path.",
            rich_help_panel="Logging",
        ),
    ] = None,
    data_extra_time: Annotated[
        float,
        Option(
            help="Additional time to take cryostat data after the "
            "action has been completed. The command will not complete until "
            "data collection has finished.",
            rich_help_panel="Logging",
        ),
    ] = 0,
):
    """Handles LN2 purges and fills.

    Allowed actions are: [bold blue]purge-and-fill[/] which executes a purge followed
    by a fill, [bold blue]purge[/] which only purges, [bold blue]fill[/] which only
    fills, [bold blue]abort[/] which aborts an ongoing purge/fill by closing all the
    valves, and [bold blue]clear[/] which removes the lock.

    """

    from logging import FileHandler

    from rich.prompt import Confirm

    from sdsstools.logger import CustomJsonFormatter, get_logger

    from lvmcryo.handlers.ln2 import LN2Handler
    from lvmcryo.notifier import Notifier
    from lvmcryo.runner import ln2_runner
    from lvmcryo.tools import LockExistsError, ensure_lock

    # Create log here and use its console for stdout.
    log = get_logger("lvmcryo", use_rich_handler=True)
    log.setLevel(5)

    stdout_console = log.rich_console
    assert stdout_console is not None

    if action == Actions.abort:
        return await _close_valves_helper()
    elif action == Actions.clear_lock:
        if LOCKFILE.exists():
            LOCKFILE.unlink()
            stdout_console.print("[green]Lock file removed.[/]")
        else:
            stdout_console.print("[yellow]No lock file found.[/]")
        return

    try:
        config = Config(
            action=action,
            cameras=cameras or [],
            config_file=config_file,
            dry_run=dry_run,
            interactive=interactive,
            no_prompt=no_prompt,
            notify=notify,
            slack=slack,
            email=email,
            email_level=email_level,
            use_thermistors=use_thermistors,
            check_pressures=check_pressures,
            check_temperatures=check_temperatures,
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
        )
    except ValueError as err:
        err_console.print(f"[red]Error parsing configuration:[/] {err}")
        return typer.Exit(1)

    if config.write_log and config.log_path:
        log.start_file_logger(str(config.log_path))

        if write_json:
            jsonHandler = FileHandler(config.log_path.with_suffix(".json"), mode="w")
            jsonHandler.setLevel(5)
            jsonHandler.setFormatter(CustomJsonFormatter())
            log.addHandler(jsonHandler)
    elif config.notify:
        # If notifying, start a file logger, but to a temporary location. This is
        # just to be able to send the log body in a notification email.
        temp_file = NamedTemporaryFile()
        log.start_file_logger(temp_file.name)

    if verbose:
        log.sh.setLevel(5)
    if quiet:
        log.sh.setLevel(30)

    # Always create a notifier. If the element is disabled the
    # notifier won't do anything. This simplifies the code.
    notifier = Notifier(config.internal_config)
    notifier.disabled = not config.notify
    notifier.slack_disabled = not config.slack
    notifier.email_disabled = not config.email

    if not config.notify:
        log.debug("Notifications are disabled and will not be emitted.")

    if not config.no_prompt:
        stdout_console.print(f"Action {config.action.value} will run with:")
        stdout_console.print(config.model_dump())
        if not Confirm.ask(
            "Continue with this configuration?",
            default=False,
            show_default=True,
            console=stdout_console,
        ):
            return typer.Exit(0)

    # Create the LN2 handler.
    handler = LN2Handler(
        config.cameras,
        interactive=config.interactive == "yes",
        log=log,
        valve_info=config.valves,
        dry_run=config.dry_run,
        alerts_route=config.internal_config["alerts_route"],
    )

    try:
        with ensure_lock(LOCKFILE):
            # Run worker.
            await ln2_runner(handler, config, notifier)

            # Check handler status.
            if handler.failed:
                raise RuntimeError(
                    "No exceptions were raised but the LN2 "
                    "handler reports a failure."
                )

            log.info(f"Event times:\n{handler.event_times.model_dump_json(indent=2)}")

    except LockExistsError:
        err_console.print(
            "[red]Lock file already exists.[/] Another instance may be "
            "performing an operation. Use [green]lvmcryo ln2 clear-lock[/] to remove "
            "the lock."
        )

        if notify:
            log.warning("Sending failure notifications.")
            await notifier.notify_failure(
                f"LN2 {action.value} failed because a lockfile was already present."
            )

        return typer.Exit(1)

    except Exception as err:
        log.info(f"Event times:\n{handler.event_times.model_dump_json(indent=2)}")

        # Close here because the stderr console messes the progress bar.
        # Probably overkill since we clear it almost everywhere in case of an error.
        await handler.clear()

        err_console.print(f"[red]Error found during {action.value}:[/] {err}")

        log.sh.setLevel(1000)  # Log the traceback to file but do not print.
        log.exception(f"Error during {action.value}.", exc_info=err)

        if notify:
            log.warning("Sending failure notifications.")
            await notifier.notify_failure(err, handler)

        if with_traceback:
            raise

        return typer.Exit(1)

    else:
        log.info(f"LN2 {action.value} completed successfully.")

        if notify and config.email_level == NotificationLevel.info:
            # The handler has already emitted a notification to Slack so just
            # send an email.
            # TODO: include log and more data here. For now it's just plain text.
            log.debug("Sending notification email.")
            notifier.send_email(
                message="The LN2 fill completed successfully.",
                subject="SUCCESS: LVM LN2 fill",
            )

    finally:
        await handler.clear()

    return typer.Exit(0)


async def _close_valves_helper():
    """Closes all the outlets."""

    from lvmcryo.handlers.valve import close_all_valves

    try:
        await close_all_valves()
        info_console.print("[green]All valves closed.[/]")
    except Exception as err:
        info_console.print(f"[red]Error closing valves:[/] {err}")
        return typer.Exit(1)

    return typer.Exit(0)


@cli.command("close-valves")
@cli_coro()
async def close_valves():
    """Closes all solenoid valves."""

    return await _close_valves_helper()


@cli.command("ion")
@cli_coro()
async def ion(
    cameras: Annotated[
        list[str] | None,
        Argument(
            help="List of cameras to handle. Defaults to all cameras.",
            case_sensitive=False,
            show_default=False,
        ),
    ] = None,
    on: Annotated[
        Optional[bool],
        Option(
            "--on/--off",
            help="Turns the ion pump on or off. If not provided, "
            "the current status of the ion pump is returned.",
            show_default=False,
        ),
    ] = None,
):
    """Controls the ion pumps.

    Without ``--on`` or ``--off``, returns the current status of the ion pump.
    A list of space-separated cameras can be provided.

    """

    from lvmopstools.devices.ion import read_ion_pumps, toggle_ion_pump

    cameras = list(map(lambda x: x.lower(), cameras)) if cameras else ["all"]

    if "all" in cameras:
        cameras = ["all"]

    if on is None:
        query_cameras = cameras if cameras != ["all"] else None
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            status = await read_ion_pumps(query_cameras)

        failed_cameras: list[str] = []
        for camera, data in status.items():
            if data["on"] is None:
                failed_cameras.append(camera)

        if len(failed_cameras) > 0:
            info_console.print(
                "[yellow]Failed getting ion pump status for cameras:[/] "
                f"{', '.join(failed_cameras)}"
            )

        status = {
            camera: data
            for camera, data in status.items()
            if camera not in failed_cameras
        }

        info_console.print(status, width=80)
        return

    error: bool = False

    for camera in cameras:
        try:
            await toggle_ion_pump(camera, on)
        except Exception as err:
            info_console.print(
                f"[yellow]Error handling ion pump for camera {camera}:[/] {err}"
            )
            error = True

    return typer.Exit(error)


if __name__ == "__main__":
    cli()
