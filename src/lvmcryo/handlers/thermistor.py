#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-26
# @Filename: thermistor.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime
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
                warnings.warn(f"Error reading thermistors: {ee}", UserWarning)
                continue

            self.data.append({"timestamp": time(), "data": data})

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
    channel
        The name of the thermistor channel to monitor.
    monitoring_interval
        The interval in seconds between thermistor checks.
    min_open_time
        The minimum valve open time. The thermistor will not be read until
        the minimum time has been reached.
    close_valve
        If ``True``, closes the valve once the thermistor is active.
    required_active_time
        Number of seconds the thermistor must be active before the valve is
        closed.
    disabled
        If ``True``, the thermistor is disabled and we will not monitor it.

    """

    valve_handler: ValveHandler
    channel: str
    monitoring_interval: float = 1.0
    min_open_time: float = 0.0
    close_valve: bool = True
    required_active_time: float = 10.0
    disabled: bool = False

    def __post_init__(self):
        self.log = self.valve_handler.log

        self.thermistor_monitor = ThermistorMonitor(interval=self.monitoring_interval)

        self.active: bool = False
        self.first_active: datetime | None = None

    async def start_monitoring(self):
        """Monitors the thermistor and potentially closes the valve."""

        # If the thermistor is not working return immediately and
        # the valve will close on a timeout.
        if self.disabled:
            self.log.warning(
                f"The thermistor for valve {self.valve_handler.valve!r} is disabled. "
                "Will not monitor it."
            )
            return

        # Unix time at which we start monitoring.
        start_time = time()
        self.thermistor_monitor.start()

        self.valve_handler.log.debug(f"Started to monitor thermistor {self.channel!r}.")

        # Unix time at which we saw the last data point.
        last_seen: float = 0

        # Unix time at which the thermistor became active.
        active_start_time: float = 0

        # Elapsed time we have been monitoring.
        elapsed_running: float = 0

        # Elapsed time the thermistor has been active
        elapsed_active: float = 0

        # How long to wait to issue a warning if ThermistorMonitor
        # is not providing new data.
        alert_seconds = int(10 * self.monitoring_interval)

        # Number of seconds since the last issued warnings.
        last_warned = -1

        while True:
            await asyncio.sleep(self.monitoring_interval)

            elapsed_running = time() - start_time

            # Check that there are measurements in the data list.
            if len(self.thermistor_monitor.data) > 0:
                # Get the last measurement.
                data_point = self.thermistor_monitor.data[-1]

                # Check that the last measurement includes the valve we are monitoring.
                if self.channel in data_point["data"]:
                    th_data = data_point["data"][self.channel]  # Boolean
                    timestamp = data_point["timestamp"]  # Unix time

                    # Check that the data is fresh.
                    if abs(timestamp - last_seen) > 0.1:
                        last_seen = timestamp

                        # If the thermistor is active, track for how long.
                        if th_data:
                            if active_start_time <= 0:
                                active_start_time = time()

                                if elapsed_running > self.min_open_time:
                                    self.log.info(
                                        f"Thermistor {self.channel!r} is active. "
                                        f"Waiting {self.required_active_time} seconds "
                                        "before closing the valve."
                                    )
                                else:
                                    self.log.warning(
                                        f"Thermistor {self.channel!r} is active but "
                                        "the minimum open time has not been reached."
                                    )

                            # Time the thermistor has been active.
                            elapsed_active = time() - active_start_time
                            if elapsed_active > self.required_active_time:
                                if not self.first_active:
                                    self.first_active = datetime.now(UTC)

                            # We require the time we have been running to
                            # be > min_open_time and the the time the thermistor
                            # has been active to be > required_active_time.
                            if (
                                active_start_time > 0
                                and elapsed_active > self.required_active_time
                                and elapsed_running > self.min_open_time
                            ):
                                # The thermistor has been active for long enough.
                                # Exit the loop and close the valve.
                                break

                        else:
                            # The thermistor is not active. Reset the active time and
                            # elapsed active time in case we had a case in which
                            # the thermistor was active for a short period and then
                            # became inactive.
                            if active_start_time > 0:
                                self.log.warning(
                                    f"Thermistor {self.channel!r} is no "
                                    "longer active. Resetting counters."
                                )

                            active_start_time = 0
                            elapsed_active = 0

            # Run some checks to be sure we are getting fresh data, but we won't
            # fail the fill if that's the case, so we only do it if there's a logger
            # to which we can report.
            if self.log is not None:
                last_seen_elapsed = time() - last_seen

                is_late = last_seen > 0 and last_seen_elapsed > alert_seconds
                never_seen = last_seen <= 0 and elapsed_running > alert_seconds

                if is_late or never_seen:
                    # Issue a warning the first time this happens
                    # and then every 30 seconds.
                    if last_warned == -1 or last_warned > 30:
                        self.log.warning(
                            f"No data from the thermistor {self.channel!r} "
                            f"in the last {last_seen_elapsed:.1f} seconds."
                        )
                        last_warned = 0
                    else:
                        last_warned += self.monitoring_interval

        self.valve_handler.log.debug(
            f"Thermistor {self.channel!r} has been active for more than "
            f"{elapsed_active:.1f} seconds."
        )
        self.active = True

        if self.close_valve:
            self.valve_handler.log.debug(
                f"Closing valve {self.valve_handler.valve!r} "
                "due to thermistor feedback."
            )
        else:
            self.valve_handler.log.warning(
                f"Thermistor {self.channel!r} is active. Calling "
                "ValveHandler.finish() but not closing the valve."
            )

        await self.valve_handler.finish(close_valve=self.close_valve)
