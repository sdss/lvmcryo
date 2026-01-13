#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import enum
import os
import pathlib
import signal
from functools import wraps

from typing import Annotated, Optional, cast

import typer
from click.core import ParameterSource
from rich.console import Console
from typer import Argument, Option
from typer.core import TyperGroup

from lvmcryo.config import Actions, InteractiveMode, NotificationLevel, ParameterOrigin


info_console = Console()
err_console = Console(stderr=True)


DEBUG = os.environ.get("LVMCRYO_DEBUG", "").lower() not in ["", "0", "false"]


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

            if loop.is_running():
                return asyncio.create_task(f(*args, **kwargs))

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


def version_callback(value: bool):
    from lvmcryo import __version__

    if value:
        typer.echo(f"lcmcryo {__version__}")
        raise typer.Exit()


@cli.callback()
def main(
    version: Annotated[
        Optional[bool],
        Option(
            "--version",
            "-V",
            help="Show the version and exit.",
            is_eager=True,
            callback=version_callback,
        ),
    ] = None,
):
    """CLI for LVM cryostats."""

    pass


@cli.command("ln2")
@cli_coro()
async def ln2(
    ctx: typer.Context,
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
            "Ignored for [green]purge[/], [green]abort[/] and [green]clear[/]. "
            "When supplied, ACTION needs to be explicitely defined.",
        ),
    ] = None,
    #
    # General options
    #
    profile: Annotated[
        Optional[str],
        Option(
            "--profile",
            "-p",
            envvar="LVMCRYO_PROFILE",
            help="Profile to use. A list of valid profiles and parameters "
            "can be printed with lvmcryo list-profiles.",
        ),
    ] = None,
    config_file: Annotated[
        Optional[pathlib.Path],
        Option(
            dir_okay=False,
            exists=True,
            envvar="LVMCRYO_CONFIG_FILE",
            help="The configuration file. Defaults to the internal configuration file.",
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
    clear_lock: Annotated[
        bool,
        Option(
            "--clear-lock",
            help="Clears the lock file if it exists before carrying out the action.",
        ),
    ] = False,
    #
    # Purge and fill options
    #
    use_thermistors: Annotated[
        bool,
        Option(
            " /--no-use-thermistors",
            envvar="LVMCRYO_USE_THERMISTORS",
            help="Use thermistor values to determine purge/fill time.",
            rich_help_panel="Purge and fill options",
        ),
    ] = True,
    require_all_thermistors: Annotated[
        bool,
        Option(
            "--require-all-thermistors",
            envvar="LVMCRYO_REQUIRE_ALL_THERMISTORS",
            help="If set, waits until all thermistors have activated before closing "
            "any of the valves. This prevents overpressures in the cryostats when only "
            "some of the valves are open. Ignore if --no-use-thermistors is used. ",
            rich_help_panel="Purge and fill options",
        ),
    ] = False,
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
    check_o2_sensors: Annotated[
        bool,
        Option(
            " /--no-check-o2-sensors",
            help="Aborts purge/fill if the oxygen sensor reads below the limit.",
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
            help="Saves the log in JSON format. Uses the same path as the file log. "
            "Ignored if --write-log is not passed.",
            rich_help_panel="Logging",
        ),
    ] = True,
    #
    # Post-fill data options
    #
    write_data: Annotated[
        bool,
        Option(
            "--write-data",
            envvar="LVMCRYO_WRITE_DATA",
            help="Saves cryostat pressures and temperatures taken during purge/fill "
            "to a parquet file.",
            rich_help_panel="Post-fill data logging",
        ),
    ] = False,
    data_path: Annotated[
        Optional[pathlib.Path],
        Option(
            envvar="LVMCRYO_DATA_PATH",
            exists=False,
            help="Path where to save the data. Implies --write-data. "
            "Defaults to a path relative to the log path.",
            rich_help_panel="Post-fill data logging",
        ),
    ] = None,
    data_extra_time: Annotated[
        float,
        Option(
            envvar="LVMCRYO_DATA_EXTRA_TIME",
            help="Additional time to take cryostat data after the "
            "action has been completed. The command will not complete until "
            "data collection has finished.",
            rich_help_panel="Post-fill data logging",
        ),
    ] = 60,
):
    """Handles LN2 purges and fills.

    Allowed actions are: [bold blue]purge-and-fill[/] which executes a purge followed
    by a fill, [bold blue]purge[/] which only purges, [bold blue]fill[/] which only
    fills, [bold blue]abort[/] which aborts an ongoing purge/fill by closing all the
    valves, and [bold blue]clear[/] which removes the lock.

    """

    from sdsstools.logger import get_logger

    from lvmcryo.runner import LN2RunnerError, ln2_runner
    from lvmcryo.tools import LockExistsError

    log = get_logger("lvmcryo", use_rich_handler=True)
    log.setLevel(5)

    try:
        # Determine parameter origins. The config uses this information to
        # decide whether a profile parameter should be overridden or not.
        param_origin: dict[str, ParameterOrigin | None] = {}
        for param in ctx.params:
            source = ctx.get_parameter_source(param)
            match source:
                case ParameterSource.COMMANDLINE:
                    param_origin[param] = ParameterOrigin.COMMAND_LINE
                case ParameterSource.DEFAULT:
                    param_origin[param] = ParameterOrigin.DEFAULT
                case ParameterSource.ENVIRONMENT:
                    param_origin[param] = ParameterOrigin.ENVVAR
                case _:
                    param_origin[param] = None

        await ln2_runner(
            action=action,
            cameras=cameras or [],
            config_file=config_file,
            dry_run=dry_run,
            clear_lock=clear_lock,
            with_traceback=with_traceback,
            interactive=interactive,
            no_prompt=no_prompt,
            notify=notify,
            slack=slack,
            email=email,
            email_level=email_level,
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
            profile=profile,
            log=log,
            __parameter_origin=param_origin,
        )

    except LockExistsError:
        err_console.print(
            "[red]Lock file already exists.[/] Another instance may be "
            "performing an operation. Use [green]lvmcryo ln2 clear-lock[/] to remove "
            "the lock."
        )
        raise typer.Exit(1)

    except Exception as err:
        # Hide the traceback unless with_traceback or DEBUG is set, but always log it
        # to the file, if we are doing that.
        log.sh.setLevel(10000)

        log.exception("Error raised in LN2 runner.", exc_info=err)
        err_console.print(f"[red]LN2 operation failed:[/] {err}")

        if (isinstance(err, LN2RunnerError) and err.propagate) or DEBUG:
            raise

        raise typer.Exit(1)

    return typer.Exit(0)


async def _close_valves_helper():
    """Closes all the outlets."""

    from lvmcryo.handlers.valve import close_all_valves

    try:
        await close_all_valves()
        info_console.print("[green]All valves closed.[/]")
    except Exception as err:
        info_console.print(f"[red]Error closing valves:[/] {err}")
        raise typer.Exit(1)

    return typer.Exit(0)


@cli.command("list-profiles")
def list_profiles(
    config_file: Annotated[
        Optional[pathlib.Path],
        Option(
            dir_okay=False,
            exists=True,
            envvar="LVMCRYO_CONFIG_FILE",
            help="The configuration file. Defaults to the internal configuration file.",
        ),
    ] = None,
):
    """Lists the available profiles."""

    from lvmcryo.config import get_internal_config

    internal_config = get_internal_config(config_file=config_file)
    profiles = internal_config["profiles"]

    for profile in profiles:
        info_console.print(profile, style="bold")
        info_console.print(profiles[profile])
        print()


@cli.command("clear-lock")
def clear_lock():
    """Clears the lock file if it exists."""

    from lvmcryo.config import get_internal_config

    config = get_internal_config()
    lockfile_path = pathlib.Path(config.get("lockfile", "/data/lvmcryo.lock"))

    if lockfile_path.exists():
        lockfile_path.unlink()
        info_console.print("[green]Lock file removed.[/]")
    else:
        info_console.print("[yellow]No lock file found.[/]")

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
    skip_pressure_check: Annotated[
        bool,
        Option(
            "--skip-pressure-check",
            help="Skips the pressure check when turning the ion pump on.",
        ),
    ] = False,
):
    """Controls the ion pumps.

    Without ``--on`` or ``--off``, returns the current status of the ion pump.
    A list of space-separated cameras can be provided.

    """

    import math
    import warnings

    from lvmopstools.devices.ion import read_ion_pumps, toggle_ion_pump
    from lvmopstools.devices.specs import Spectrographs, spectrograph_pressures

    from lvmcryo.config import get_internal_config

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

        status_pretty = {}
        for camera, data in status.items():
            if camera not in failed_cameras:
                pressure = data["pressure"]
                status_pretty[camera] = {
                    "pressure": float(f"{pressure:.2e}")  # Truncate to 2 decimals
                    if pressure is not None
                    else math.nan,
                    "on": data["on"],
                }

        info_console.print(status_pretty, width=80)
        return

    error: bool = False

    if cameras == ["all"]:
        config = get_internal_config()
        cameras = config["defaults"]["cameras"]

    if cameras is None:
        err_console.print("[red]No cameras were specified.[/]")
        raise typer.Exit(1)

    for camera in cameras:
        try:
            if on is True and not skip_pressure_check:
                spec = f"sp{camera[-1]}"
                pressures = await spectrograph_pressures(cast(Spectrographs, spec))
                cam_pressure = pressures.get(camera, None)
                if cam_pressure is None:
                    raise ValueError(f"Could not get pressure for {camera!r}.")

                if cam_pressure > 1e-4:
                    raise ValueError(
                        f"Camera {camera} has a pressure of {cam_pressure:.2e} "
                        "Torr. If you want to turn it on use --skip-pressure-check."
                    )

            await toggle_ion_pump(camera, on)
        except Exception as err:
            info_console.print(
                f"[yellow]Error handling ion pump for camera {camera}:[/] {err}"
            )
            error = True

    return typer.Exit(error)


class AutoFillAction(enum.Enum):
    """Auto-fill actions."""

    enable = "enable"
    disable = "disable"
    restart = "restart"


@cli.command("auto-fill")
def auto_fill(
    action: Annotated[
        AutoFillAction,
        Argument(
            help="Auto-fill action.",
            case_sensitive=False,
        ),
    ],
):
    """Enables or disables the auto-fill procedure."""

    from lvmcryo.tools import run_command

    CRONJOB_PATH = "/home/sdss5/config/kube/cronjobs/ln2fill_2_fills.yml"

    try:
        if action == AutoFillAction.enable:
            run_command(
                ["kubectl", "apply", "-f", CRONJOB_PATH],
                output_on_error=True,
                raise_on_error=True,
            )
            info_console.print("[green]Auto-fill enabled.[/]")
        elif action == AutoFillAction.disable:
            run_command(
                ["kubectl", "delete", "-f", CRONJOB_PATH],
                output_on_error=True,
                raise_on_error=True,
            )
            info_console.print("[yellow]Auto-fill disabled.[/]")
        elif action == AutoFillAction.restart:
            run_command(
                ["kubectl", "replace", "--force", "-f", CRONJOB_PATH],
                output_on_error=True,
                raise_on_error=True,
            )
            info_console.print("[green]Auto-fill restarted.[/]")
    except FileNotFoundError:
        err_console.print(
            "[red]kubectl not found. Is Kubernetes installed on this system?[/]"
        )
        raise typer.Exit(1)
    except RuntimeError:
        # Just exit with error. run_commnad should have output the error message.
        raise typer.Exit(1)


if __name__ == "__main__":
    cli()
