#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
import signal
import sys
import warnings

from typing import Unpack

import click

from sdsstools import Configuration, read_yaml_file
from sdsstools.daemonizer import cli_coro

from ln2fill.tools import is_container

from . import config, log
from .types import OptionsType


VALID_ACTIONS = ["purge-and-fill", "purge", "fill", "abort", "clear"]

LOCKFILE = pathlib.Path("/data/ln2fill.lock")


def update_options(
    ctx: click.Context,
    use_defaults: bool,
    configuration_file: str | None = None,
    **options: Unpack[OptionsType],
):
    """Updates the input options using defaults."""

    if use_defaults is True and configuration_file is not None:
        raise click.UsageError(
            "--use-internal and --configuration-file are mutually exclusive.",
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
    for option in options:
        source = ctx.get_parameter_source(option)
        if source == click.core.ParameterSource.COMMANDLINE:
            continue

        if (default_value := defaults[option]) is not None:
            options[option] = default_value

    # Now go one by one over the options that may need to be adjusted.
    if options["cameras"] is None:
        options["cameras"] = internal_config["defaults.cameras"]

    for option in [
        "max_pressure",
        "max_temperature",
        "min_purge_time",
        "max_purge_time",
        "min_fill_time",
        "max_fill_time",
    ]:
        value = options[option]

        if isinstance(value, (float, int)):
            continue
        elif value is None:
            minmax, label = option.split("_")[0:2]
            internal_value = internal_config[f"limits.{label}.{minmax}"]
            if internal_value is None:
                raise ValueError(f"Cannot find internal value for {option!r},")
            options[option] = internal_value
        elif isinstance(value, str) and value.startswith("{{"):
            param = value.strip("{}")
            options[option] = internal_config[f"{param}"]
        else:
            raise ValueError(f"Invalid value {value} for parameter {option!r}.")

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Define log path.
    if options["write_log"] or options["log_path"]:
        options["write_log"] = True
        if options["log_path"]:
            options["log_path"] = options["log_path"].format(timestamp=timestamp)
            if not options["log_path"].endswith(".log"):
                options["log_path"] += "/ln2fill.log"
        else:
            options["log_path"] = "./ln2fill.log"

    log_path = pathlib.Path(options["log_path"]) if options["log_path"] else None

    # Define measurements path.
    if options["write_measurements"] or options["measurements_path"]:
        options["write_measurements"] = True
        if options["measurements_path"] is None and log_path:
            options["measurements_path"] = str(log_path.parent / "measurements.parquet")
        else:
            options["measurements_path"] = "./ln2fill.parquet"

    # Define the QA path
    if options["generate_qa"] or options["qa_path"]:
        options["generate_qa"] = True
        if options["qa_path"] is None and log_path:
            options["qa_path"] = str(log_path.parent)
        else:
            options["qa_path"] = "./"

    # Determine if we should run interactively.
    if options["interactive"] == "auto":
        if is_container():
            options["interactive"] = "no"
        else:
            options["interactive"] = "yes"
    elif options["interactive"] == "yes":
        if is_container():
            warnings.warn("Interactive mode may not work in containers.", UserWarning)

    if options["no_prompt"] is None:
        if options["interactive"] == "yes":
            options["no_prompt"] = True
        else:
            options["no_prompt"] = False

    if (
        options["no_prompt"]
        and options["use_thermistors"] is False
        and (options["purge_time"] is None or options["fill_time"] is None)
    ):
        raise ValueError(
            "Cannot run without thermistors and without purge and fill times."
        )

    return options.copy()


async def handle_fill(**options: Unpack[OptionsType]):
    """Handles the purge/fill process."""

    from ln2fill.handlers import LN2Handler

    if options["quiet"]:
        log.sh.setLevel(logging.ERROR)
    elif options["verbose"]:
        log.sh.setLevel(logging.DEBUG)

    if options["write_log"]:
        assert options["log_path"]
        log_path = pathlib.Path(options["log_path"])
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log.start_file_logger(str(log_path), mode="w", rotating=False)

    if options["cameras"] is None:
        raise RuntimeError("No cameras specified.")

    if isinstance(options["cameras"], str):
        cameras = list(map(lambda s: s.strip(), options["cameras"].split(",")))
    else:
        cameras = options["cameras"]

    interactive = True if options["interactive"] == "yes" else False

    handler = LN2Handler(cameras=cameras, interactive=interactive)

    await handler.check()

    async def signal_handler():
        log.error("User aborted the process. Closing all valves before exiting.")
        await handler.abort(only_active=False)
        log.error("All valves closed. Exiting.")
        asyncio.get_running_loop().call_soon(sys.exit, 0)

    for signame in ("SIGINT", "SIGTERM"):
        asyncio.get_running_loop().add_signal_handler(
            getattr(signal, signame),
            lambda: asyncio.create_task(signal_handler()),
        )

    max_purge_time = options["purge_time"] or options["max_purge_time"]
    await handler.purge(
        use_thermistor=options["use_thermistors"],
        min_purge_time=options["min_purge_time"],
        max_purge_time=max_purge_time,
        prompt=not options["no_prompt"],
    )

    max_fill_time = options["fill_time"] or options["max_fill_time"]
    await handler.fill(
        use_thermistors=options["use_thermistors"],
        min_fill_time=options["min_fill_time"],
        max_fill_time=max_fill_time,
        prompt=not options["no_prompt"],
    )


@click.command(name="ln2fill")
@click.argument(
    "ACTION",
    type=click.Choice(VALID_ACTIONS, case_sensitive=False),
    default="purge-and-fill",
)
@click.option(
    "--use-defaults",
    "-D",
    is_flag=True,
    help="Uses the internal configuration file to set the default values. "
    "Options explicitely defined will still override the defaults.",
)
@click.option(
    "--configuration-file",
    type=click.Path(exists=True, dir_okay=False),
    help="The configuration file to use to set the default values and other options. "
    "Incompatible with --use-defaults.",
)
@click.option(
    "--cameras",
    "-c",
    type=str,
    help="Comma-separated cameras to fill. Defaults to all cameras.",
)
@click.option(
    "--interactive",
    "-i",
    type=click.Choice(["auto", "yes", "no"], case_sensitive=False),
    default="auto",
    help="Controls whether the interactive features are shown. When --interactive auto "
    "(the default), interactivity is determined based on console mode.",
)
@click.option(
    "--no-prompt",
    is_flag=True,
    help="Does not prompt the user to finish or abort a purge/fill. Prompting is "
    "required if --fill-time or --purge-time are not provided and thermistors "
    "are not used.",
)
@click.option(
    "--check-pressure/--no-check-pressure",
    default=True,
    show_default=True,
    help="Aborts purge/fill if the pressure of any cryostat is above the limit.",
)
@click.option(
    "--check-temperature/--no-check-temperature",
    default=True,
    show_default=True,
    help="Aborts purge/fill if the temperature of a fill cryostat is above the limit.",
)
@click.option(
    "--max-pressure",
    type=float,
    help="Maximum cryostat pressure if --check-pressure. Defaults to internal value.",
)
@click.option(
    "--max-temperature",
    type=float,
    help="Maximum cryostat temperature if --check-temperature. "
    "Defaults to internal value.",
)
@click.option(
    "--purge-time",
    type=float,
    help="Purge time in seconds. If --use-thermistors, this is effectively the "
    "maximum purge time.",
)
@click.option(
    "--min-purge-time",
    type=float,
    help="Minimum purge time in seconds. Defaults to internal value.",
)
@click.option(
    "--max-purge-time",
    type=float,
    help="Maximum purge time in seconds. Defaults to internal value.",
)
@click.option(
    "--fill-time",
    type=float,
    help="Fill time in seconds. If --use-thermistors, this is effectively the "
    "maximum fill time.",
)
@click.option(
    "--min-fill-time",
    type=float,
    help="Minimum fill time in seconds. Defaults to internal value.",
)
@click.option(
    "--max-fill-time",
    type=float,
    help="Maximum fill time in seconds. Defaults to internal value.",
)
@click.option(
    "--use-thermistors/--no-use-thermistors",
    default=True,
    show_default=True,
    help="Use thermistor values to determine purge/fill time.",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Disable logging to stdout.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Outputs additional information to stdout.",
)
@click.option(
    "--write-json",
    is_flag=True,
    help="Writes a JSON file with the run configuration and status. "
    "Defaults to a path relative to --log-path if defined, or the current directory.",
)
@click.option(
    "--write-log",
    "-S",
    is_flag=True,
    help="Saves the log to a file.",
)
@click.option(
    "--log-path",
    type=str,
    help="Path where to save the log file. Implies --write-log. "
    "Defaults to the current directory.",
)
@click.option(
    "--write-measurements",
    is_flag=True,
    help="Saves cryostat pressures and temperatures taken during purge/fill "
    "to a parquet file.",
)
@click.option(
    "--measurements-path",
    type=str,
    help="Path where to save the measurements. Implies --write-measurements. "
    "Defaults to a path relative to --write-log-path, if provided, or to the "
    "current directory.",
)
@click.option(
    "--measurements-extra-time",
    type=float,
    default=0,
    help="Additional time to take cryostat measurements after the "
    "action has been completed.",
)
@click.option(
    "--generate-qa",
    is_flag=True,
    help="Generates QA plots.",
)
@click.option(
    "--qa-path",
    type=str,
    help="Path where to save the QA files. Implies --generate-qa. "
    "Defaults to a path relative to --save-log-path, if provided, or to the "
    "current directory.",
)
@click.option(
    "--slack",
    is_flag=True,
    help="Notifies the Slack channel.",
)
@click.option(
    "--slack-route",
    type=str,
    help="The API route to use to send messages to Slack.",
)
@click.option(
    "--email",
    is_flag=True,
    help="Notifies over email after completion or failure.",
)
@click.option(
    "--email-recipients",
    type=str,
    help="Comma-separated list of email recipients. Required if --email is set.",
)
@click.pass_context
@cli_coro()
async def ln2fill_cli(
    ctx,
    action: str,
    use_defaults: bool = False,
    configuration_file: str | None = None,
    **options: Unpack[OptionsType],
):
    """CLI for the LN2 purge and fill utilities."""

    if action == "abort":
        pass
    elif action == "clear":
        LOCKFILE.unlink(missing_ok=True)
        return

    options = update_options(
        ctx=ctx,
        use_defaults=use_defaults,
        configuration_file=configuration_file,
        **options,
    )

    await handle_fill(**options)


def main():
    ln2fill_cli()


if __name__ == "__main__":
    main()
