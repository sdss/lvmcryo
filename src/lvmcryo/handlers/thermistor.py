#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-26
# @Filename: thermistor.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import logging
import warnings
from dataclasses import dataclass
from time import time

from typing import TYPE_CHECKING, TypedDict

from lvmopstools.devices.thermistors import read_thermistors


if TYPE_CHECKING:
    from lvmcryo.handlers.valve import ValveHandler


class ThermistorDataPoint(TypedDict):
    """A data point from the thermistor server."""

    timestamp: float
    data: dict[str, bool]


class ThermistorMonitor:
    """Queries all thermistors and stores data points. Implemented as a singleton."""

    __instance: ThermistorMonitor | None = None
    __initialised: bool = False

    def __new__(cls, interval: float = 1):
        if cls.__instance is None:
            cls.clear()

            cls.__instance = super(ThermistorMonitor, cls).__new__(cls)
            cls.__instance.__initialised = False

        return cls.__instance

    def __init__(self, interval: float = 1):
        if self.__initialised:
            return

        self.__initialised = True

        self.interval = interval

        self.data: list[ThermistorDataPoint] = []
        self._task: asyncio.Task | None = None

    def start(self):
        """Starts the monitoring task."""

        if self._task and not self._task.done():
            return

        self._task = asyncio.create_task(self._monitor())

    def stop(self):
        """Stops the monitoring task."""

        if self._task and not self._task.done():
            self._task.cancel()

    async def _monitor(self):
        """Monitors the thermistors and emits an event when new data is available."""

        while True:
            try:
                data: dict[str, bool] = await read_thermistors()
            except Exception as ee:
                warnings.warn(f"Error reading thermistors: {ee}")
                continue

            self.data.append({"timestamp": time(), "data": data})

            await asyncio.sleep(0.01)

            await asyncio.sleep(self.interval)

    @classmethod
    def clear(cls):
        if cls.__instance:
            cls.__instance.stop()

        cls.__instance = None
        cls.__initialised = False


@dataclass
class ThermistorHandler:
    """Reads a thermistor, potentially closing the valve if active.

    Parameters
    ----------
    valve_handler
        The `.ValveHandler` instance associated with this thermistor.
    interval
        The interval in seconds between thermistor checks.
    min_open_time
        The minimum valve open time. The thermistor will not be read until
        the minimum time has been reached.
    close_valve
        If ``True``, closes the valve once the thermistor is active.
    min_active_time
        Number of seconds the thermistor must be active before the valve is
        closed.
    log
        A logger instance.

    """

    valve_handler: ValveHandler
    interval: float = 1.0
    min_open_time: float = 0.0
    close_valve: bool = True
    min_active_time: float = 10.0
    log: logging.Logger | None = None

    def __post_init__(self):
        self.channel = self.valve_handler.thermistor_channel or self.valve_handler.valve

        # Unix time at which we start monitoring.
        self._start_time: float = -1

        # Unix time at which we saw the last data point.
        self._last_seen: float = -1

        # Unix time at which the thermistor became active.
        self._active_time: float = -1

        self.thermistor_monitor = ThermistorMonitor(interval=self.interval)

    async def start_monitoring(self):
        """Monitors the thermistor and potentially closes the valve."""

        self._start_time = time()
        self.thermistor_monitor.start()

        self.valve_handler.log.debug(f"Starting to monitor thermistor {self.channel}.")

        elapsed: float = -1

        while True:
            await asyncio.sleep(self.interval)

            if len(self.thermistor_monitor.data) > 0:
                data_point = self.thermistor_monitor.data[-1]

                if self.channel in data_point["data"]:
                    th_data = data_point["data"][self.channel]
                    timestamp = data_point["timestamp"]

                    if abs(timestamp - self._last_seen) > 0.1:
                        self._last_seen = timestamp
                    if th_data:
                        if self._active_time < 0:
                            self._active_time = time()

                        elapsed = time() - self._active_time

                        if (
                            self._active_time > 0
                            and elapsed > self.min_active_time
                            and elapsed > self.min_open_time
                        ):
                            # The thermistor has been active for long enough.
                            # Exit the loop and close the valve.
                            break

            # Run some checks to be sure we are getting fresh data, but we won't
            # fail the fill if that's the case, so we only do it if there's a logger.
            if self.log is not None:
                last_seen_elapsed = time() - self._last_seen
                monitoring_elapsed = time() - self._start_time

                alert_seconds = int(10 * self.interval)

                is_late = self._last_seen > 0 and last_seen_elapsed > alert_seconds
                never_seen = self._last_seen < 0 and monitoring_elapsed > alert_seconds

                if is_late or never_seen:
                    self.log.warning(
                        f"No data from the thermistor {self.channel} "
                        f"in the last {alert_seconds} seconds."
                    )

            await asyncio.sleep(self.interval)

        self.valve_handler.log.debug(
            f"Thermistor {self.channel} has been active for more than "
            f"{elapsed:.1f} seconds."
        )

        if self.close_valve:
            self.valve_handler.log.debug(f"Closing valve {self.valve_handler.valve}.")
            await self.valve_handler.finish()
