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
import warnings

import click
from click_help_colors import HelpColorsGroup
from click_option_group import optgroup
from pydantic import BaseModel

from sdsstools.daemonizer import cli_coro


VALID_ACTIONS = ["purge-and-fill", "purge", "fill", "abort", "clear"]

LOCKFILE = pathlib.Path("/data/lvmcryo.lock")


class OptionsModel(BaseModel):
    """CLI options."""

    cameras: str | None
    interactive: str
    no_prompt: bool | None
    check_pressure: bool
    check_temperature: bool
    max_pressure: float
    min_pressure: float
    max_temperature: float
    min_temperature: float
    purge_time: float | None
    min_purge_time: float
    max_purge_time: float
    fill_time: float | None
    min_fill_time: float
    max_fill_time: float
    use_thermistors: bool
    verbose: bool
    quiet: bool
    write_json: bool
    json_path: str | None
    write_log: bool
    log_path: str | None
    write_measurements: bool
    measurements_path: str | None
    measurements_extra_time: float
    generate_qa: bool
    qa_path: str | None
    notify: bool
    dry_run: bool


def process_cli_options(
    ctx: click.Context,
    options: OptionsModel,
    use_defaults: bool,
    configuration_file: str | None = None,
):
    """Updates the input options using defaults."""

    import datetime

    from sdsstools import Configuration, read_yaml_file

    from lvmcryo import config
    from lvmcryo.tools import is_container

    if use_defaults is True and configuration_file is not None:
        raise click.UsageError(
            "--use-defaults and --configuration-file are mutually exclusive.",
        )

    # Read internal configuration. We'll use this as a last resource in some cases,
    # regardless of other defaults.
    internal_config = read_yaml_file(pathlib.Path(__file__).parent / "config.yaml")

    # Select the source of defaults, if any.
    if use_defaults:
        defaults = Configuration(internal_config["defaults"])

    elif configuration_file is not None:
        new_config = read_yaml_file(configuration_file)

        # Get defaults from the new configuration file. This fully overrides
        # any internal defaults.
        defaults = Configuration(new_config.get("defaults", {}))

        # Update global config.
        config._BASE_CONFIG_FILE = config._CONFIG_FILE
        config._CONFIG_FILE = configuration_file
        config.reload()

    else:
        defaults = Configuration({})

    # Update options. If the option value comes from the command line (i.e.,
    # explicitely defined by the user), leave it be. Otherwise use the default.
    for option in options.model_fields_set:
        source = ctx.get_parameter_source(option)
        if source == click.core.ParameterSource.COMMANDLINE:
            continue

        if (default_value := defaults[option]) is not None:
            setattr(options, option, default_value)

    # Now go one by one over the options that may need to be adjusted.
    if options.cameras is None:
        options.cameras = internal_config["defaults.cameras"]

    for option in [
        "max_pressure",
        "max_temperature",
        "min_purge_time",
        "max_purge_time",
        "min_fill_time",
        "max_fill_time",
    ]:
        value = getattr(options, option)

        if isinstance(value, (float, int)):
            continue
        elif value is None:
            minmax, label = option.split("_")[0:2]
            internal_value = internal_config[f"limits.{label}.{minmax}"]
            if internal_value is None:
                raise ValueError(f"Cannot find internal value for {option!r},")
            setattr(options, option, internal_value)
        elif isinstance(value, str) and value.startswith("{{"):
            param = value.strip("{}")
            setattr(options, option, internal_config[f"{param}"])
        else:
            raise ValueError(f"Invalid value {value} for parameter {option!r}.")

    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Define log path.
    if options.write_log or options.log_path:
        options.write_log = True
        if options.log_path:
            options.log_path = options.log_path.format(timestamp=timestamp)
            if not options.log_path.endswith(".log"):
                options.log_path += "/lvmcryo.log"
        else:
            options.log_path = "./lvmcryo.log"

    log_path = pathlib.Path(options.log_path) if options.log_path else None

    # Define measurements path.
    if options.write_measurements or options.measurements_path:
        options.write_measurements = True
        if options.measurements_path is None:
            if log_path:
                options.measurements_path = str(log_path.parent / "lvmcryo.parquet")
            else:
                options.measurements_path = "./lvmcryo.parquet"

    # Define the QA path
    if options.generate_qa or options.qa_path:
        options.generate_qa = True
        if options.qa_path is None:
            if log_path:
                options.qa_path = str(log_path.parent)
            else:
                options.qa_path = "./"

    # Define the JSON path
    if options.write_json or options.write_json:
        options.write_json = True
        if options.json_path is None:
            if log_path is not None:
                options.json_path = str(log_path.parent / "lvmcryo.json")
            else:
                options.json_path = "./lvmcryo.json"

    # Determine if we should run interactively.
    if options.interactive == "auto":
        if is_container():
            options.interactive = "no"
        else:
            options.interactive = "yes"
    elif options.interactive == "yes":
        if is_container():
            warnings.warn("Interactive mode may not work in containers.", UserWarning)

    if options.no_prompt is None:
        if options.interactive == "yes":
            options.no_prompt = True
        else:
            options.no_prompt = False

    if (
        options.no_prompt
        and options.use_thermistors is False
        and (options.purge_time is None or options.fill_time is None)
    ):
        raise ValueError(
            "Cannot run without thermistors and without purge and fill times."
        )

    return options


@click.group(
    cls=HelpColorsGroup,
    help_headers_color="yellow",
    help_options_color="green",
)
def cli():
    """LVM cryostat-related tools."""


@cli.command(name="fill")
@click.argument(
    "ACTION",
    type=click.Choice(VALID_ACTIONS, case_sensitive=False),
    default="purge-and-fill",
)
@optgroup.group("Configuration options")
@optgroup.option(
    "--use-defaults",
    "-D",
    is_flag=True,
    help="Uses the internal configuration file to set the default values. "
    "Options explicitely defined will still override the defaults.",
)
@optgroup.option(
    "--configuration-file",
    type=click.Path(exists=True, dir_okay=False),
    help="The configuration file to use to set the default values and other options. "
    "Incompatible with --use-defaults.",
)
@optgroup.option(
    "--interactive",
    "-i",
    type=click.Choice(["auto", "yes", "no"], case_sensitive=False),
    default="auto",
    help="Controls whether the interactive features are shown. When --interactive auto "
    "(the default), interactivity is determined based on console mode.",
)
@optgroup.option(
    "--notify/--no-notify",
    is_flag=True,
    default=True,
    help="Whether to send notifications of success/failure to Slack and over email.",
)
@optgroup.option(
    "--no-prompt",
    is_flag=True,
    help="Does not prompt the user to finish or abort a purge/fill. Prompting is "
    "required if --fill-time or --purge-time are not provided and thermistors "
    "are not used.",
)
@optgroup.option(
    "--dry-run",
    is_flag=True,
    help="Test run the code but do not actually open/close any valves.",
)
@optgroup.group(
    "Purge and fill options",
    help="Configuration for the purge and fill process.",
)
@optgroup.option(
    "--cameras",
    "-c",
    type=str,
    help="Comma-separated cameras to fill. Defaults to all cameras.",
)
@optgroup.option(
    "--check-pressure/--no-check-pressure",
    default=True,
    show_default=True,
    help="Aborts purge/fill if the pressure of any cryostat is above the limit.",
)
@optgroup.option(
    "--check-temperature/--no-check-temperature",
    default=True,
    show_default=True,
    help="Aborts purge/fill if the temperature of a fill cryostat is above the limit.",
)
@optgroup.option(
    "--max-pressure",
    type=float,
    help="Maximum cryostat pressure if --check-pressure. Defaults to internal value.",
)
@optgroup.option(
    "--max-temperature",
    type=float,
    help="Maximum cryostat temperature if --check-temperature. "
    "Defaults to internal value.",
)
@optgroup.option(
    "--purge-time",
    type=float,
    help="Purge time in seconds. If --use-thermistors, this is effectively the "
    "maximum purge time.",
)
@optgroup.option(
    "--min-purge-time",
    type=float,
    help="Minimum purge time in seconds. Defaults to internal value.",
)
@optgroup.option(
    "--max-purge-time",
    type=float,
    help="Maximum purge time in seconds. Defaults to internal value.",
)
@optgroup.option(
    "--fill-time",
    type=float,
    help="Fill time in seconds. If --use-thermistors, this is effectively the "
    "maximum fill time.",
)
@optgroup.option(
    "--min-fill-time",
    type=float,
    help="Minimum fill time in seconds. Defaults to internal value.",
)
@optgroup.option(
    "--max-fill-time",
    type=float,
    help="Maximum fill time in seconds. Defaults to internal value.",
)
@optgroup.option(
    "--use-thermistors/--no-use-thermistors",
    default=True,
    show_default=True,
    help="Use thermistor values to determine purge/fill time.",
)
@optgroup.group("Logging options")
@optgroup.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Disable logging to stdout.",
)
@optgroup.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Outputs additional information to stdout.",
)
@optgroup.option(
    "--write-log",
    "-S",
    is_flag=True,
    help="Saves the log to a file.",
)
@optgroup.option(
    "--log-path",
    type=str,
    help="Path where to save the log file. Implies --write-log. "
    "Defaults to the current directory.",
)
@optgroup.option(
    "--write-json",
    is_flag=True,
    help="Writes a JSON file with the run configuration and status. "
    "Defaults to a path relative to --log-path if defined, or the current directory.",
)
@optgroup.option(
    "--json-path",
    type=str,
    help="Path where to save the JSON data file. Implies --write-json. "
    "Defaults to a path relative to --write-log-path, if provided, or to the "
    "current directory.",
)
@optgroup.option(
    "--write-measurements",
    is_flag=True,
    help="Saves cryostat pressures and temperatures taken during purge/fill "
    "to a parquet file.",
)
@optgroup.option(
    "--measurements-path",
    type=str,
    help="Path where to save the measurements. Implies --write-measurements. "
    "Defaults to a path relative to --write-log-path, if provided, or to the "
    "current directory.",
)
@optgroup.option(
    "--measurements-extra-time",
    type=float,
    default=0,
    help="Additional time to take cryostat measurements after the "
    "action has been completed.",
)
@optgroup.option(
    "--generate-qa",
    is_flag=True,
    help="Generates QA plots.",
)
@optgroup.option(
    "--qa-path",
    type=str,
    help="Path where to save the QA files. Implies --generate-qa. "
    "Defaults to a path relative to --save-log-path, if provided, or to the "
    "current directory.",
)
@click.pass_context
@cli_coro()
async def fill_cli(
    ctx,
    action: str,
    use_defaults: bool = False,
    configuration_file: str | None = None,
    **kwargs,
):
    """Run the LN2 purge/fill routines."""

    import logging
    from tempfile import NamedTemporaryFile

    from lvmcryo import log
    from lvmcryo.handlers.ln2 import LN2Handler
    from lvmcryo.notifier import Notifier
    from lvmcryo.runner import collect_mesurement_data, ln2_runner, notify_failure
    from lvmcryo.tools import JSONWriter, close_all_valves

    error: Exception | None = None

    if action == "abort":
        log.warning("Closing all valves.")
        await close_all_valves()
        return
    elif action == "clear":
        LOCKFILE.unlink(missing_ok=True)
        return

    options = process_cli_options(
        ctx,
        OptionsModel(**kwargs),
        use_defaults=use_defaults,
        configuration_file=configuration_file,
    )

    if options.quiet:
        log.sh.setLevel(logging.ERROR)
    elif options.verbose:
        log.sh.setLevel(logging.DEBUG)

    if options.write_log and options.log_path:
        log_path = pathlib.Path(options.log_path)
    else:
        # Write log to a temporary path so we can recover it for notifications.
        log_path = pathlib.Path(NamedTemporaryFile(suffix=".log", delete=True).name)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log.start_file_logger(str(log_path), mode="w", rotating=False)

    if options.cameras is None:
        raise RuntimeError("No cameras specified.")

    if isinstance(options.cameras, str):
        cameras = list(map(lambda s: s.strip(), options.cameras.split(",")))
    else:
        cameras = options.cameras

    interactive = True if options.interactive == "yes" else False

    handler = LN2Handler(cameras=cameras, interactive=interactive)
    notifier = Notifier(silent=not options.notify)

    try:
        await ln2_runner(handler, options, notifier=notifier)

    except Exception as err:
        JSONWriter(options.json_path, "times", handler.event_times.model_dump_json())

        if handler.failed:
            JSONWriter(options.json_path, "status", "failed")

        await notify_failure(err, ln2_handler=handler, notifier=notifier)

        error = err

    await asyncio.sleep(options.measurements_extra_time)
    if options.write_measurements and options.measurements_path:
        measurements_path = pathlib.Path(options.measurements_path)
        await collect_mesurement_data(handler, path=measurements_path)

    if error is not None:
        raise error


@cli.command(name="ion")
@click.argument("CAMERAS", type=str, nargs=-1, required=True)
@click.option(
    "--on/--off",
    default=None,
    is_flag=True,
    help="Turns the ion pump on or off.",
)
@cli_coro()
async def lvm_ion_cli(cameras: list[str], on: bool | None = None):
    """Controls the ion pumps.

    Without ``--on`` or ``--off``, returns the current status of the ion pump.
    A list of space-separated cameras can be provided. A special camera ``ALL``
    can be used to control all cameras.

    """

    from lvmcryo import config, log
    from lvmcryo.ion import read_ion_pump, toggle_pump

    cameras = list(map(lambda x: x.lower(), cameras))
    if "all" in cameras:
        cameras = list(config["ion"].keys())

    for camera in cameras:
        try:
            if on is None:
                status = await read_ion_pump(camera)
                log.info(
                    f"{camera} - "
                    f"Pressure: {status['pressure']:.3g} Torr - "
                    f"On: {status['is_on']}"
                )
            else:
                await toggle_pump(camera, on=on)

        except Exception as err:
            log.warning(f"Error handling ion pump for camera {camera}: {err}")


def main():
    cli(max_content_width=100)


if __name__ == "__main__":
    main()
