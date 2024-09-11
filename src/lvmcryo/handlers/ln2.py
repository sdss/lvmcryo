#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-26
# @Filename: ln2.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import datetime

from typing import Coroutine

import sshkeyboard
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from sdsstools.configuration import RecursiveDict

from lvmcryo import config, log
from lvmcryo.handlers.valve import ValveHandler
from lvmcryo.tools import (
    get_spectrograph_status,
    read_thermistors,
)


class EventDict(BaseModel):
    """Dictionary of events."""

    purge_start: datetime.datetime | None = None
    purge_complete: datetime.datetime | None = None
    fill_start: datetime.datetime | None = None
    fill_complete: datetime.datetime | None = None
    failed: datetime.datetime | None = None
    aborted: datetime.datetime | None = None


@dataclass
class LN2Handler:
    """The main LN2 purge/fill handlerclass.

    Parameters
    ----------
    cameras
        List of cameras to fill. Defaults to all the cameras.
    purge_valve
        The name of the purge valve.
    interactive
        Whether to show interactive features.

    """

    cameras: list[str] | None = None
    purge_valve: str = "purge"
    interactive: bool = False

    def __post_init__(self):
        if self.cameras is None:
            self.cameras = list(config["defaults.cameras"])

        self._valve_handlers: dict[str, ValveHandler] = {}
        for camera in self.cameras + [self.purge_valve]:
            self._valve_handlers[camera] = ValveHandler(camera)

        self.event_times = EventDict()
        self.failed: bool = False

    def get_specs(self):
        """Returns a list of spectrographs being handled."""

        assert self.cameras is not None

        specs: set[str] = set([])
        for camera in self.cameras:
            cam_id = camera[-1]
            specs.add(f"sp{cam_id}")

        return specs

    async def check(
        self,
        max_pressure: float | None = None,
        max_temperature: float | None = None,
        check_thermistors: bool = True,
    ):
        """Checks if the pressure and temperature are in the allowed range.

        If ``max_temperature`` or ``max_pressure`` are not provided, the values
        from the configuration file are used.

        Parameters
        ----------
        max_pressure
            The maximum pressure to allow.
        max_temperature
            The maximum temperature to allow.
        check_thermistors
            Checks that the thermistors are reading correctly.

        Raises
        ------
        RuntimeError
            If any of the checks fails.

        """

        log.info("Checking pressure and temperature ...")

        assert self.cameras is not None

        if max_pressure is None:
            max_pressure = config["limits.pressure.max"]
        if max_temperature is None:
            max_temperature = config["limits.temperature.max"]

        assert max_pressure is not None and max_temperature is not None

        specs = self.get_specs()

        try:
            spec_status = await get_spectrograph_status(list(specs))
            if not isinstance(spec_status, dict):
                raise RuntimeError("Invalid spectrograph response.")
        except Exception as err:
            raise RuntimeError(f"Failed reading spectrograph status: {err}")

        spec_status = RecursiveDict(spec_status)

        for camera in self.cameras:
            ln2_temp = spec_status[f"{camera}.ln2"]
            if ln2_temp is None:
                self.failed = True
                raise RuntimeError(f"Invalid {camera!r} temperature.")
            if ln2_temp > max_temperature:
                self.failed = True
                raise RuntimeError(
                    f"LN2 temperature for camera {camera} is {ln2_temp:.1f} K "
                    f"which is above the maximum allowed temperature "
                    f"({max_temperature:.1f} K)."
                )

            pressure = spec_status[f"{camera}.pressure"]
            if pressure is None:
                self.failed = True
                raise RuntimeError(f"Invalid {camera!r} pressure.")
            if pressure > max_pressure:
                self.failed = True
                raise RuntimeError(
                    f"Pressure for camera {camera} is {pressure} K "
                    f"which is above the maximum allowed pressure "
                    f"({max_pressure})."
                )

        if check_thermistors:
            log.info("Checking thermistors ...")

            try:
                thermistors = await read_thermistors()
                assert isinstance(thermistors, dict), "invalid return type."
            except Exception as err:
                self.failed = True
                raise RuntimeError(f"Failed reading thermistors: {err}")

            for valve in self._valve_handlers:
                channel = self._valve_handlers[valve].thermistor_channel
                assert channel is not None, "invalid thermistor channel."

                thermistor = thermistors[channel]
                if thermistor is None:
                    self.failed = True
                    raise RuntimeError(f"Invalid {valve!r} thermistor.")
                if thermistor is True:
                    self.failed = True
                    raise RuntimeError(f"Thermistor for valve {valve} is active.")

        log.info("All checks passed.")

        return True

    def _get_now(self):
        """Returns a UTC datetime for now."""

        return datetime.datetime.now(datetime.UTC)

    async def purge(
        self,
        purge_valve: str | None = None,
        use_thermistor: bool = True,
        min_purge_time: float | None = None,
        max_purge_time: float | None = None,
        prompt: bool | None = None,
        dry_run: bool = False,
    ):
        """Purges the system.

        Parameters
        ----------
        purge_valve
            The name of the purge valve. If ``None``, uses the instance default.
        use_thermistor
            Whether to use the thermistor to close the valve.
        min_purge_time
            The minimum time to keep the purge valve open. Only relevant if
            using the thermistor.
        max_purge_time
            The maximum time to keep the purge valve open. If
            ``use_thermistor=None`` this is effectively the purge time unless
            ``prompt=True`` and the purge is cancelled before reaching the
            timeout.
        prompt
            Whether to show a prompt to stop or cancel the purge. If ``None``,
            determined from the instance ``interactive`` attribute.
        dry_run
            If True, does not actually purge. All checks are perfomed and the
            hardware is expected to be connected.

        """

        if purge_valve is None:
            purge_valve = self.purge_valve

        valve_handler = self._valve_handlers[purge_valve]
        min_open_time = min_purge_time or 0.0

        self.event_times.purge_start = self._get_now()

        log.info(
            f"Beginning purge using valve {valve_handler.valve!r} with "
            f"use_thermistor={use_thermistor}, min_open_time={min_open_time}, "
            f"timeout={max_purge_time}."
        )

        prompt = prompt if prompt is not None else self.interactive
        if prompt:
            log.warning('Press "x" to abort or "enter" to finish the purge.')
            self._kb_monitor()

        try:
            if dry_run is False:
                await valve_handler.start_fill(
                    min_open_time=min_open_time,
                    timeout=max_purge_time,
                    use_thermistor=use_thermistor,
                )
            log.info("Purge complete.")
            self.event_times.purge_complete = self._get_now()
        except Exception:
            self.failed = True
            raise
        finally:
            if prompt:
                sshkeyboard.stop_listening()

    async def fill(
        self,
        cameras: list[str] | None = None,
        use_thermistors: bool = True,
        min_fill_time: float | None = None,
        max_fill_time: float | None = None,
        prompt: bool | None = None,
        dry_run: bool = False,
    ):
        """Fills the selected cameras.

        Parameters
        ----------
        cameras
            The list of cameras to fill. If not provided, defaults to the values
            used to instantiate the `.LN2Handler` instance.
        use_thermistors
            Whether to use the thermistors to close the valves.
        min_fill_time
            The minimum time to keep the fill valves open. Only relevant if
            using the thermistors.
        max_fill_time
            The maximum time to keep the fill valves open. If
            ``use_thermistor=None`` this is effectively the fill time unless
            ``prompt=True`` and the fills are cancelled before reaching the
            timeout.
        prompt
            Whether to show a prompt to stop or cancel the fill. If ``None``,
            determined from the instance ``interactive`` attribute.
        dry_run
            If True, does not actually fill. All checks are perfomed and the
            hardware is expected to be connected.

        """

        cameras = cameras or self.cameras
        if cameras is None or len(cameras) == 0:
            raise RuntimeError("No cameras selected for filling.")

        min_open_time = min_fill_time or 0.0

        fill_tasks: list[Coroutine] = []

        for camera in cameras:
            try:
                valve_handler = self._valve_handlers[camera]
            except KeyError:
                raise RuntimeError(f"Unable to find valve for camera {camera!r}.")

            fill_tasks.append(
                valve_handler.start_fill(
                    min_open_time=min_open_time,
                    timeout=max_fill_time,
                    use_thermistor=use_thermistors,
                )
            )

        self.event_times.fill_start = self._get_now()

        log.info(
            f"Beginning fill on cameras {cameras!r} with "
            f"use_thermistors={use_thermistors}, min_open_time={min_open_time}, "
            f"timeout={max_fill_time}."
        )

        prompt = prompt if prompt is not None else self.interactive
        if prompt:
            log.warning('Press "x" to abort or "enter" to finish the purge.')
            self._kb_monitor()

        try:
            if dry_run is False:
                await asyncio.gather(*fill_tasks)
            log.info("Fill complete.")
            self.event_times.fill_complete = self._get_now()
        except Exception:
            self.failed = True
            raise
        finally:
            if prompt:
                sshkeyboard.stop_listening()

    def _kb_monitor(self):
        """Monitors the keyboard and cancels/aborts the fill.."""

        async def monitor_keys(key: str):
            """Parses a pressed key and cancels/aborts the fills."""

            if key not in ["x", "X", "enter"]:
                return

            if key == "x" or key == "X":
                log.warning("Aborting purge/fill.")
                await self.abort(only_active=False)
            elif key == "enter":
                await self.abort(only_active=True)

            sshkeyboard.stop_listening()

        # No need to store this task. It will be automatically done when
        # sshkeyboard.stop_listening() is called.
        asyncio.create_task(sshkeyboard.listen_keyboard_manual(on_press=monitor_keys))

    async def abort(self, only_active: bool = True):
        """Cancels ongoing fills and closes the valves.

        If ``only_active=True`` only active valves will be closed. Otherwise
        closes all valves.

        """

        tasks: list[Coroutine] = []

        for valve_handler in self._valve_handlers.values():
            if valve_handler.active or not only_active:
                tasks.append(valve_handler.finish_fill())

        await asyncio.gather(*tasks)

        self.event_times.aborted = self._get_now()
