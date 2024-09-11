#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-26
# @Filename: valve.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import dataclasses

from typing import Optional

from pydantic import model_validator
from pydantic.dataclasses import dataclass
from rich.progress import TaskID

from lvmcryo import config, log
from lvmcryo.tools import (
    TimerProgressBar,
    cancel_nps_threads,
    cancel_task,
    valve_on_off,
)


PROGRESS_BAR = TimerProgressBar(console=log.rich_console)


@dataclass
class ValveHandler:
    """Handles a valve, including opening and closing, timeouts, and thermistors.

    Parameters
    ----------
    valve
        The name of the valve. Additional information such as thermistor channel,
        actor names, etc. are determined from the configuration file if not
        explicitely provided.
    thermistor_channel
        The channel of the thermistor associated with the valve.
    nps_actor
        The NPS actor that command the valve.
    show_progress_bar
        Whether to show a progress bar during fills.

    """

    valve: str
    thermistor_channel: Optional[str] = dataclasses.field(default=None, repr=False)
    nps_actor: Optional[str] = dataclasses.field(default=None, repr=False)
    show_progress_bar: Optional[bool] = dataclasses.field(default=True, repr=False)

    @model_validator(mode="after")
    def validate_fields(self):
        if self.valve not in config["valves"]:
            raise ValueError(f"Unknown valve {self.valve!r}.")

        if self.thermistor_channel is None:
            thermistor_key = f"valves.{self.valve}.thermistor"
            self.thermistor_channel = config.get(thermistor_key, self.valve)

        if self.nps_actor is None:
            self.nps_actor = config.get(f"valves.{self.valve}.actor")
            if self.nps_actor is None:
                raise ValueError(f"Cannot find NPS actor for valve {self.valve!r}.")

        return self

    def __post_init__(self):
        from lvmcryo.handlers.thermistor import ThermistorHandler

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

        await self._set_state(True, timeout=timeout, use_script=True)

        if use_thermistor:
            self.thermistor.min_open_time = min_open_time
            self._monitor_task = asyncio.create_task(self.thermistor.start_monitoring())

        if self.show_progress_bar:
            if self.valve.lower() == "purge":
                initial_description = "Purge in progress ..."
                complete_description = "Purge complete"
            else:
                initial_description = "Fill in progress ..."
                complete_description = "Fill complete"

            self._progress_bar_id = await PROGRESS_BAR.add_timer(
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
        await self.finish_fill()

    async def finish_fill(self):
        """Finishes the fill, closing the valve."""

        await self._set_state(False)

        await cancel_task(self._monitor_task)
        self._monitor_task = None

        if self._progress_bar_id is not None:
            await PROGRESS_BAR.stop_timer(self._progress_bar_id)
            self._progress_bar_id = None

            await cancel_task(self._timeout_task)
            self._timeout_task = None

        if not self.event.is_set():
            self.event.set()

        self.active = False

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

        assert self.nps_actor is not None

        # If there's already a thread running for this valve, we cancel it.
        if self._thread_id is not None:
            await cancel_nps_threads(self.nps_actor, self._thread_id)

        thread_id = await valve_on_off(
            self.valve,
            on,
            timeout=timeout,
            use_script=use_script,
        )

        if thread_id is not None:
            self._thread_id = thread_id

        if on:
            if thread_id is not None:
                log.debug(
                    f"Valve {self.valve!r} was opened with timeout={timeout} "
                    f"(thread_id={thread_id})."
                )
        else:
            log.debug(f"Valve {self.valve!r} was closed.")
