#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-12-03
# @Filename: runner.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import datetime
import json
import pathlib
import signal
import sys
import traceback

import pygments
from pygments.formatters import HtmlFormatter
from pygments.lexers.python import PythonTracebackLexer
from pygments.lexers.rust import RustLexer

from lvmcryo import log
from lvmcryo.__main__ import OptionsModel
from lvmcryo.handlers import LN2Handler
from lvmcryo.notifier import Notifier
from lvmcryo.tools import (
    JSONWriter,
    close_all_valves,
    get_spectrograph_status,
    render_template,
)


__all__ = ["ln2_runner"]


async def signal_handler(handler: LN2Handler):
    log.error("User aborted the process. Closing all valves before exiting.")
    await handler.abort(only_active=False)
    log.error("All valves have been closed. Exiting.")
    asyncio.get_running_loop().call_soon(sys.exit, 0)


async def ln2_runner(
    handler: LN2Handler,
    options: OptionsModel,
    notifier: Notifier | None = None,
):
    """Runs the purge/fill process.

    This function is usually called by the CLI.

    """

    notifier = notifier or Notifier(silent=not options.notify)

    # Record options used.
    log.info("Starting LN2 fill process with options:")
    for key, value in options.model_dump().items():
        log.info(f"  {key}: {value}")

    # Initial version of the JSON file.
    if options.json_path:
        json.dump(
            {
                "runner": {"options": options, "dry_run": options.dry_run},
                "times": handler.event_times.model_dump_json(),
                "status": "pending",
                "checks": "pending",
                "cryostat_status": {"before_purge": None, "after_fill": None},
            },
            open(options.json_path, "w"),
            indent=4,
        )

    spec_status = await get_spectrograph_status(list(handler.get_specs()))
    JSONWriter(options.json_path, "cryostat_status.before_purge", spec_status)

    # Inform of the start of the fill in Slack.
    now = handler._get_now()
    now_str = now.strftime("%H:%M:%S")
    if options.use_thermistors:
        await notifier.send_to_slack(f"Starting LN2 fill at {now_str}.")
    else:
        await notifier.send_to_slack(
            f"Starting LN2 fill at {now_str} with "
            f"purge_time={options.purge_time} and "
            f"fill_time={options.fill_time}."
        )

    await handler.check(
        max_pressure=options.max_pressure,
        max_temperature=options.max_temperature,
        check_thermistors=options.use_thermistors,
    )

    JSONWriter(options.json_path, "checks", "passed")

    # Register signals that will trigger a valve shutdown and clean exit.
    for signame in ("SIGINT", "SIGTERM"):
        asyncio.get_running_loop().add_signal_handler(
            getattr(signal, signame),
            lambda: asyncio.create_task(signal_handler(handler)),
        )

    if not options.dry_run:
        log.info("Closing all valves before purging/filling.")
        await close_all_valves()

    max_purge_time = options.purge_time or options.max_purge_time
    await handler.purge(
        use_thermistor=options.use_thermistors,
        min_purge_time=options.min_purge_time,
        max_purge_time=max_purge_time,
        prompt=not options.no_prompt,
        dry_run=options.dry_run,
    )
    JSONWriter(options.json_path, "times", handler.event_times.model_dump_json())

    max_fill_time = options.fill_time or options.max_fill_time
    await handler.fill(
        use_thermistors=options.use_thermistors,
        min_fill_time=options.min_fill_time,
        max_fill_time=max_fill_time,
        prompt=not options.no_prompt,
        dry_run=options.dry_run,
    )

    await notifier.send_to_slack("LN2 fill completed successfully.")

    JSONWriter(options.json_path, "times", handler.event_times.model_dump_json())
    JSONWriter(options.json_path, "status", "success")
    handler.failed = False  # Just in case.

    spec_status = await get_spectrograph_status(list(handler.get_specs()))
    JSONWriter(options.json_path, "cryostat_status.after_fill", spec_status)


async def collect_mesurement_data(
    handler: LN2Handler,
    path: pathlib.Path | None = None,
    backlog_time: float = 60.0,
):
    """Collects and writes cryostat data for the period of the fill."""

    if handler.event_times.purge_start is not None:
        start_time = handler.event_times.purge_start
    elif handler.event_times.fill_start is not None:
        start_time = handler.event_times.fill_start
    else:
        raise RuntimeError("No purge or fill start time found.")

    start_time -= datetime.timedelta(seconds=backlog_time)


async def notify_failure(
    error: str | Exception | None = None,
    ln2_handler: LN2Handler | None = None,
    notifier: Notifier | None = None,
    channels: list[str] = ["slack", "email"],
    include_status: bool = True,
    include_log: bool = True,
):
    """Notifies a fill failure to the users."""

    notifier = notifier or Notifier()

    log.error("Something went wrong. Sending alerts.")

    if include_status:
        spec_data = await get_spectrograph_status()
    else:
        spec_data = None

    formatter = HtmlFormatter(style="default")

    if include_log and log.log_filename is not None and log.fh is not None:
        log.fh.flush()
        log_data = open(log.log_filename, "r").read()
        log_blob = pygments.highlight(log_data, RustLexer(), formatter)
    else:
        log_blob = None

    if error is not None:
        if isinstance(error, Exception):
            error = pygments.highlight(
                "".join(traceback.format_exception(error)),
                PythonTracebackLexer(),
                formatter,
            )
    assert isinstance(error, str) or error is None

    for channel in channels:
        if channel == "slack":
            await notifier.send_to_slack(
                text="Something went wrong with the LN2 fill. "
                "Please check the status of the spectrographs.",
                is_alert=True,
            )

        elif channel == "email":
            message = render_template(
                "alert_message.html",
                render_data=dict(
                    event_times=ln2_handler.event_times if ln2_handler else {},
                    spec_data=spec_data,
                    log_blob=log_blob,
                    log_css=formatter.get_style_defs(),
                    error=error,
                ),
            )
            notifier.send_email(subject="LVM LN2 fill failed", message=message)
