#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-26
# @Filename: thermistor.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
from time import time

from pydantic import Field
from pydantic.dataclasses import dataclass

from ln2fill.handlers.valve import ValveHandler
from ln2fill.tools import read_thermistors


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
    min_active_time: float = Field(default=10.0, ge=0.0)

    def __init__(self, *args, **kwargs):
        self.valve_handler = self.valve_handler

        # Unix time at which we started to monitor.
        self._start_time: float = -1

        # Unix time at which the thermistor became active.
        self._active_time: float = -1

        thermistor_channel = self.valve_handler.thermistor_channel
        assert thermistor_channel

        self.thermistor_channel: str = thermistor_channel

    async def read_thermistor(self):
        """Reads the thermistor and returns its state."""

        thermistors = await read_thermistors()
        return thermistors[self.thermistor_channel]

    async def start_monitoring(self):
        """Monitors the thermistor and potentially closes the valve."""

        await asyncio.sleep(self.min_open_time)

        while True:
            thermistor_status = await self.read_thermistor()
            if thermistor_status is True:
                if self._active_time < 0:
                    self._active_time = time.time()
                if time.time() - self._active_time > self.min_active_time:
                    break

            await asyncio.sleep(self.interval)

        if self.close_valve:
            await self.valve_handler.finish_fill()
