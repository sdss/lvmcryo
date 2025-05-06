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
from functools import wraps

from typing import Annotated, Optional, cast

import typer
from rich.console import Console
from typer import Argument, Option
from typer.core import TyperGroup

from lvmcryo.config import Actions, InteractiveMode, NotificationLevel
from lvmcryo.tools import DBHandler


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

    from logging import FileHandler
    from tempfile import NamedTemporaryFile

    from rich.prompt import Confirm

    from sdsstools.logger import get_logger

    from lvmcryo import __version__
    from lvmcryo.config import Config
    from lvmcryo.handlers.ln2 import LN2Handler, get_now
    from lvmcryo.notifier import Notifier
    from lvmcryo.runner import ln2_runner, post_fill_tasks
    from lvmcryo.tools import (
        LockExistsError,
        add_json_handler,
        ensure_lock,
    )
    from lvmcryo.validate import validate_fill

    # Create log here and use its console for stdout.
    log = get_logger("lvmcryo", use_rich_handler=True)
    log.setLevel(5)

    stdout_console = log.rich_console
    assert stdout_console is not None

    error: Exception | str | None = None
    skip_finally: bool = False

    json_path: pathlib.Path | None = None
    json_handler: FileHandler | None = None
    images: dict[str, pathlib.Path | None] = {}

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
            # We cannot pass the context directly so we pass a dict of the
            # origin of each parameter to reject profile parameters that
            # have been manually defined.
            param_source={pp: ctx.get_parameter_source(pp) for pp in ctx.params},
        )
    except ValueError as err:
        err_console.print(f"[red]Error parsing configuration:[/] {err}")
        raise typer.Exit(1)

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
    notifier = Notifier(config.internal_config)
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
            return typer.Exit(0)

    # Create the LN2 handler.
    handler = LN2Handler(
        config.cameras,
        interactive=config.interactive == "yes",
        log=log,
        valve_info=config.valve_info,
        dry_run=config.dry_run,
        alerts_route=config.internal_config["api_routes"]["alerts"],
    )

    if LOCKFILE.exists() and config.clear_lock:
        log.warning("Lock file exists. Removing it because --clear-lock.")
        LOCKFILE.unlink()

    db_handler = DBHandler(action, handler, config, json_handler=json_handler)
    record_pk = await db_handler.write(complete=False)
    if record_pk:
        log.debug(f"Record {record_pk} created in the database.")

    try:
        with ensure_lock(LOCKFILE):
            # Calculate the expected maximum run time.
            max_time: float = 2 * 3600  # It should never take longer than two hours.
            if config.max_purge_time is not None and config.max_fill_time is not None:
                max_time = config.max_purge_time + config.max_fill_time + 300.0

            # Run worker.
            await asyncio.wait_for(
                ln2_runner(handler, config, notifier, db_handler=db_handler),
                timeout=max_time,
            )

            # Check handler status.
            if handler.failed:
                raise RuntimeError(
                    "No exceptions were raised but the LN2 handler reports a failure."
                )

    except LockExistsError:
        err_console.print(
            "[red]Lock file already exists.[/] Another instance may be "
            "performing an operation. Use [green]lvmcryo ln2 clear-lock[/] to remove "
            "the lock."
        )

        if config.notify:
            log.warning("Sending failure notifications.")
            await notifier.notify_after_fill(
                False,
                error_message=f"LN2 {action.value} failed because a lockfile "
                "was already present.",
            )

        # Do not do anything special for this error, just exit.
        skip_finally = True

        raise typer.Exit(1)

    except Exception as err:
        # Log the traceback to file but do not print.
        orig_sh_level = log.sh.level
        log.sh.setLevel(1000)
        log.exception(f"Error during {action.value}.", exc_info=err)
        log.sh.setLevel(orig_sh_level)

        # Fail the action.
        handler.failed = True

        error = err
        if config.with_traceback:
            raise

        raise typer.Exit(1)

    else:
        log.info(f"LN2 {action.value} completed successfully.")

    finally:
        # At this point all the valves are closed so we can remove the signal handlers.
        for signame in ("SIGINT", "SIGTERM"):
            asyncio.get_running_loop().remove_signal_handler(getattr(signal, signame))

        handler.event_times.end_time = get_now()
        await handler.clear()

        log.info(f"Event times:\n{handler.event_times.model_dump_json(indent=2)}")

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
                api_data_route=config.internal_config["api_routes"]["fill_data"],
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
                    error = validate_error
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
                # Last message before existing.
                err_console.print(f"[red]Error found during {action.value}:[/] {error}")

    if error:
        raise typer.Exit(1)


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


if __name__ == "__main__":
    cli()
