#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-11
# @Filename: ln2fill.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import dataclasses
from time import time

from typing import Coroutine, Optional

import sshkeyboard
from pydantic import Field, model_validator
from pydantic.dataclasses import dataclass
from rich.progress import TaskID

from sdsstools.configuration import RecursiveDict

from ln2fill import config, log
from ln2fill.tools import (
    TimerProgressBar,
    cancel_nps_threads,
    cancel_task,
    get_spectrograph_status,
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

        specs: set[str] = set([])
        for camera in self.cameras:
            cam_id = camera[-1]
            specs.add(f"sp{cam_id}")

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
                raise RuntimeError(f"Invalid {camera!r} temperature.")
            if ln2_temp > max_temperature:
                raise RuntimeError(
                    f"LN2 temperature for camera {camera} is {ln2_temp:.1f} K "
                    f"which is above the maximum allowed temperature "
                    f"({max_temperature:.1f} K)."
                )

            pressure = spec_status[f"{camera}.pressure"]
            if pressure is None:
                raise RuntimeError(f"Invalid {camera!r} pressure.")
            if pressure > max_pressure:
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
                raise RuntimeError(f"Failed reading thermistors: {err}")

            for valve in self._valve_handlers:
                channel = self._valve_handlers[valve].thermistor_channel
                assert channel is not None, "invalid thermistor channel."

                thermistor = thermistors[channel]
                if thermistor is None:
                    raise RuntimeError(f"Invalid {valve!r} thermistor.")
                if thermistor is True:
                    raise RuntimeError(f"Thermistor for valve {valve} is active.")

        log.info("All checks passed.")

        return True

    async def purge(
        self,
        purge_valve: str | None = None,
        use_thermistor: bool = True,
        min_purge_time: float | None = None,
        max_purge_time: float | None = None,
        prompt: bool | None = None,
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


        """

        if purge_valve is None:
            purge_valve = self.purge_valve

        valve_handler = self._valve_handlers[purge_valve]
        min_open_time = min_purge_time or 0.0

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
            await valve_handler.start_fill(
                min_open_time=min_open_time,
                timeout=max_purge_time,
                use_thermistor=use_thermistor,
            )
            log.info("Purge complete.")
        except Exception:
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
            await asyncio.gather(*fill_tasks)
            log.info("Fill complete.")
        except Exception:
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
