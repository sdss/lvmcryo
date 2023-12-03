#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-12-03
# @Filename: runner.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import logging
import pathlib
import signal
import sys

from typing import Unpack

from ln2fill import log
from ln2fill.handlers import LN2Handler
from ln2fill.types import OptionsType


__all__ = ["ln2_runner"]


async def signal_handler(handler: LN2Handler):
    log.error("User aborted the process. Closing all valves before exiting.")
    await handler.abort(only_active=False)
    log.error("All valves have been closed. Exiting.")
    asyncio.get_running_loop().call_soon(sys.exit, 0)


async def ln2_runner(**options: Unpack[OptionsType]):
    """Runs the purge/fill process.

    This function is usually called by the CLI.

    """

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

    # Register signals that will trigger a valve shutdown and clean exit.
    for signame in ("SIGINT", "SIGTERM"):
        asyncio.get_running_loop().add_signal_handler(
            getattr(signal, signame),
            lambda: asyncio.create_task(signal_handler(handler)),
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
