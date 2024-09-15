#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-26
# @Filename: valve.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from typing import TYPE_CHECKING, Optional

from rich.progress import TaskID

from lvmcryo.handlers.thermistor import ThermistorHandler
from lvmcryo.tools import (
    cancel_nps_threads,
    cancel_task,
    get_fake_logger,
    valve_on_off,
)


if TYPE_CHECKING:
    from lvmcryo.tools import TimerProgressBar


@dataclass
class ValveHandler:
    """Handles a valve, including opening and closing, timeouts, and thermistors.

    Parameters
    ----------
    valve
        The name of the valve. Additional information such as thermistor channel,
        actor names, etc. are determined from the configuration file if not
        explicitely provided.
    actor
        The NPS actor that command the valve.
    outlet
        The outlet name of the valve in the actor.
    thermistor_channel
        The channel name of the thermistor connected to the valve.
    progress_bar
        Progress bar instance used to display progress.

    """

    valve: str
    actor: str
    outlet: str
    thermistor_channel: str | None = None
    progress_bar: Optional[TimerProgressBar] = None
    log: logging.Logger = field(default_factory=get_fake_logger)

    def __post_init__(self):
        self.thermistor = ThermistorHandler(self)

        self._thread_id: int | None = None
        self._progress_bar_id: TaskID | None = None

        self._monitor_task: asyncio.Task | None = None
        self._timeout_task: asyncio.Task | None = None

        self.event = asyncio.Event()
        self.active: bool = False

    async def start_fill(
        self,
        min_open_time: float = 0.0,
        timeout: float | None = None,
        use_thermistor: bool = True,
    ):
        """Starts a fill.

        Parameters
        ----------
        min_open_time
            Minimum time to keep the valve open.
        timeout
            The maximumtime to keep the valve open. If ``None``, defaults to
            the ``max_open_time`` value.
        use_thermistor
            Whether to use the thermistor to close the valve. If ``True`` and
            ``fill_time`` is not ``None``, ``fill_time`` become the maximum
            open time.

        """

        if timeout is None:
            # Some hardcoded hard limits.
            if self.valve == "purge":
                timeout = 2000
            else:
                timeout = 600

            self.log.warning(
                f"No timeout provided for valve {self.valve!r}. "
                f"Using default of {timeout} seconds."
            )

        await self._set_state(True, timeout=timeout, use_script=True)

        if use_thermistor:
            self.thermistor.min_open_time = min_open_time
            self._monitor_task = asyncio.create_task(self.thermistor.start_monitoring())

        if self.progress_bar:
            if self.valve.lower() == "purge":
                initial_description = "Purge in progress ..."
                complete_description = "Purge complete"
            else:
                initial_description = "Fill in progress ..."
                complete_description = "Fill complete"

            self._progress_bar_id = await self.progress_bar.add_timer(
                timeout,
                label=self.valve,
                initial_description=initial_description,
                complete_description=complete_description,
            )

        await asyncio.sleep(2)
        self._timeout_task = asyncio.create_task(self._schedule_timeout_task(timeout))

        self.active = True

        self.event.clear()
        await self.event.wait()

    async def _schedule_timeout_task(self, timeout: float):
        """Schedules a task to cancel the fill after a timeout."""

        await asyncio.sleep(timeout)
        await self.finish()

    async def finish(self):
        """Finishes the fill, closing the valve."""

        await self._set_state(False)

        self._monitor_task = await cancel_task(self._monitor_task)
        self._timeout_task = await cancel_task(self._timeout_task)

        if self._progress_bar_id is not None and self.progress_bar is not None:
            await self.progress_bar.stop_timer(self._progress_bar_id)
            self._progress_bar_id = None

        self.active = False

        if not self.event.is_set():
            self.event.set()

    async def _set_state(
        self,
        on: bool,
        use_script: bool = True,
        timeout: float | None = None,
    ):
        """Sets the state of the valve.

        Parameters
        ----------
        on
            Whether to open or close the valve.
        use_script
            Whether to use the ``cycle_with_timeout`` script to set a timeout
            after which the valve will be closed.
        timeout
            Maximum open valve time.

        """

        assert self.actor is not None

        # If there's already a thread running for this valve, we cancel it.
        if self._thread_id is not None:
            await cancel_nps_threads(self.actor, self._thread_id)

        thread_id = await valve_on_off(
            self.actor,
            self.valve,
            on,
            timeout=timeout,
            use_script=use_script,
        )

        if thread_id is not None:
            self._thread_id = thread_id

        if on:
            if thread_id is not None:
                self.log.info(
                    f"Valve {self.valve!r} was opened with timeout={timeout} "
                    f"(thread_id={thread_id})."
                )
        else:
            self.log.info(f"Valve {self.valve!r} was closed.")
