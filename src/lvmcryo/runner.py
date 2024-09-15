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
import signal
import sys

from typing import TYPE_CHECKING

from lvmcryo.config import Actions
from lvmcryo.handlers import LN2Handler
from lvmcryo.notifier import Notifier
from lvmcryo.tools import close_all_valves


if TYPE_CHECKING:
    from lvmcryo.config import Config


__all__ = ["ln2_runner"]


async def signal_handler(handler: LN2Handler, log: logging.Logger):
    """Handles signals to close all valves and exit cleanly."""

    log.error("User aborted the process. Closing all valves before exiting.")
    await handler.abort(only_active=False)

    log.error("All valves have been closed. Exiting.")
    asyncio.get_running_loop().call_soon(sys.exit, 0)


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

    # Inform of the start of the fill in Slack.
    now = handler._get_now()
    now_str = now.strftime("%H:%M:%S")

    action = config.action.value

    if config.use_thermistors or config.purge_time is None or config.fill_time is None:
        await notifier.post_to_slack(f"Starting LN₂ {action} at {now_str}.")
    else:
        await notifier.post_to_slack(
            f"Starting LN₂ {action} at {now_str} with "
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

    if not config.dry_run:
        log.info(f"Closing all valves before {action}.")
        await close_all_valves()

    if config.action == Actions.purge_fill or config.action == Actions.purge:
        await notifier.post_to_slack("Starting purge.")
        max_purge_time = config.purge_time or config.max_purge_time
        await handler.purge(
            use_thermistor=config.use_thermistors,
            min_purge_time=config.min_purge_time,
            max_purge_time=max_purge_time,
            prompt=not config.no_prompt,
            dry_run=config.dry_run,
        )

    if config.action == Actions.purge_fill or config.action == Actions.fill:
        await notifier.post_to_slack("Starting fill.")
        max_fill_time = config.fill_time or config.max_fill_time
        await handler.fill(
            use_thermistors=config.use_thermistors,
            min_fill_time=config.min_fill_time,
            max_fill_time=max_fill_time,
            prompt=not config.no_prompt,
            dry_run=config.dry_run,
        )

    await notifier.post_to_slack("LN₂ {action} completed successfully.")

    handler.failed = False  # Just in case.
