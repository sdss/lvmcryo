#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-11
# @Filename: ln2fill.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import logging
from time import time

from pydantic import Field, model_validator
from pydantic.dataclasses import dataclass

from sdsstools.logger import SDSSLogger, get_logger

from ln2fill import config
from ln2fill.tools import (
    cancel_nps_threads,
    is_container,
    read_thermistors,
    valve_on_off,
)


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
            await self.valve_handler.set_state(False)


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

    """

    valve: str
    thermistor_channel: str = Field(None, repr=False)
    nps_actor: str = Field(None, repr=False)
    max_open_time: float | None = Field(None, ge=5.0, repr=False)

    @model_validator(mode="after")
    def validate_valve(self):
        if self.valve not in config["valves"]:
            raise ValueError(f"Unknown valve {self.valve!r}.")

        if self.thermistor_channel is None:
            thermistor_key = f"valves.{self.valve}.thermistor"
            self.thermistor_channel = config.get(thermistor_key, self.valve)

        if self.nps_actor is None:
            self.nps_actor = config.get(f"valves.{self.valve}.actor")
            if self.nps_actor is None:
                raise ValueError("Cannot find NPS actor for valve {self.valve!r}.")

        return self

    def __post_init__(self):
        self.thermistor = ThermistorHandler(self)

        self._thread_id: int | None = None

    async def set_state(
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
    log
        A logger instance. Must be an ``sdsstools.logger.SDSSLogger`` instance
        or `None`, in which case a new logger will be created.
    quiet
        If `True`, only outputs error messages.

    """

    def __init__(
        self,
        interactive: bool | None = None,
        log: SDSSLogger | None = None,
        quiet: bool = False,
    ):
        self.log = log or get_logger("lvm-ln2fill", use_rich_handler=True)
        if quiet:
            self.log.sh.setLevel(logging.ERROR)

        if interactive is None:
            self.interactive = False if is_container() else True
        else:
            self.interactive = interactive
