#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-11
# @Filename: ln2fill.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
from time import time

from pydantic import Field, model_validator
from pydantic.dataclasses import dataclass
from rich.progress import TaskID

from ln2fill import config, log
from ln2fill.tools import (
    TimerProgressBar,
    cancel_nps_threads,
    is_container,
    read_thermistors,
    valve_on_off,
)


PROGRESS_BAR = TimerProgressBar(console=log.rich_console)


@dataclass
class ThermistorHandler:
    """Reads a thermistor, potentially closing the valve if active.

    Parameters
    ----------
    valve_handler
        The `.ValveHandler` instance associated with this thermistor.
    interval
        The interval in seconds between thermistor reads.
    min_open_time
        The minimum valve open time. The thermistor will not be read until
        the minimum time has been reached.
    close_valve
        If ``True``, closes the valve once the thermistor is active.
    min_active_time
        Number of seconds the thermistor must be active before the valve is
        closed.

    """

    valve_handler: ValveHandler
    interval: float = 1
    min_open_time: float = Field(default=0.0, ge=0.0)
    close_valve: bool = True
    min_active_time: float = Field(default=0.0, ge=0.0)

    def __init__(self, *args, **kwargs):
        self.valve_handler = self.valve_handler

        # Unix time at which we started to monitor.
        self._start_time: float = -1

        # Unix time at which the thermistor became active.
        self._active_time: float = -1

        self.thermistor_channel: str = self.valve_handler.thermistor_channel

    async def read_thermistor(self):
        """Reads the thermistor and returns its state."""

        thermistors = await read_thermistors()
        return thermistors[self.thermistor_channel]

    async def start_monitoring(
        self,
        close_valve: bool | None = None,
        min_active_time: float | None = None,
    ):
        """Monitors the thermistor and potentially closes the valve.

        Sets the `.Future` as complete once the active state has been reached.
        ``close_valve`` and ``min_active_time`` can be set to override the
        instance values.

        """

        close_valve = close_valve if close_valve is not None else self.close_valve

        if min_active_time is not None:
            assert min_active_time >= 0
        else:
            min_active_time = self.min_active_time

        await asyncio.sleep(self.min_open_time)

        while True:
            thermistor_status = await self.read_thermistor()
            if thermistor_status is True:
                if self._active_time < 0:
                    self._active_time = time.time()
                if time.time() - self._active_time > min_active_time:
                    break

            await asyncio.sleep(self.interval)

        if close_valve:
            await self.valve_handler.finish_fill()


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
    thermistor_channel: str = Field(None, repr=False)
    nps_actor: str = Field(None, repr=False)
    max_open_time: float = Field(None, ge=5.0, repr=False)
    show_progress_bar: bool = Field(True, repr=False)

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
                raise ValueError("Cannot find NPS actor for valve {self.valve!r}.")

        # Some reasonable, last resource values.
        if self.max_open_time is None:
            if self.valve == "purge":
                self.max_open_time = 2000
            else:
                self.max_open_time = 600

        return self

    def __post_init__(self):
        self.thermistor = ThermistorHandler(self)

        self._thread_id: int | None = None
        self._progress_bar_id: TaskID | None = None

    async def start_fill(
        self,
        fill_time: float | None = None,
        use_thermistor: bool = True,
    ):
        """Starts a fill.

        Parameters
        ----------
        fill_time
            The time to keep the valve open. If ``None``, defaults to
            the ``max_open_time`` value.
        use_thermistor
            Whether to use the thermistor to close the valve. If ``True`` and
            ``fill_time`` is not ``None``, ``fill_time`` become the maximum
            open time.

        """

        if use_thermistor:
            if fill_time is not None:
                timeout = fill_time
            else:
                timeout = self.max_open_time
        else:
            if fill_time is None:
                raise ValueError("fill_time is required with use_thermistor=False")
            timeout = fill_time

        await self._set_state(True, timeout=timeout, use_script=True)

        if use_thermistor:
            await self.thermistor.start_monitoring()

        if self.show_progress_bar:
            if self.valve.lower() == "purge":
                initial_description = "Purge in progress ..."
                complete_description = "Purge complete"
            else:
                initial_description = "Fill in progress ..."
                complete_description = "Fill complete"

            self._progress_bar_id = await PROGRESS_BAR.add_timer(
                self.max_open_time,
                label=self.valve,
                initial_description=initial_description,
                complete_description=complete_description,
            )

    async def finish_fill(self):
        """Finishes the fill, closing the valve."""

        await self._set_state(False)

        if self._progress_bar_id is not None:
            await PROGRESS_BAR.stop_timer(self._progress_bar_id)
            self._progress_bar_id = None

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
            Maximum open valve time. If ``None``, defaults to the ``max_open_time``
            value.

        """

        if timeout is None or timeout < 0:
            timeout = self.max_open_time

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


class LN2Handler:
    """The main LN2 purge/fill handlerclass.

    Parameters
    ----------
    interactive
        Whether to show interactive features. If ``None``, interactivity will
        be determined depending on the console type.
    """

    def __init__(self, interactive: bool | None = None):
        if interactive is None:
            self.interactive = False if is_container() else True
        else:
            self.interactive = interactive
