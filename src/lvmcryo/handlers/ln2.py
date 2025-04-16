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
import logging
from dataclasses import dataclass, field

from typing import Any, Callable, Coroutine, Literal, NoReturn, overload

import sshkeyboard
from pydantic import BaseModel, field_serializer
from rich import box
from rich.align import Align
from rich.console import Console
from rich.markup import render
from rich.panel import Panel

from lvmopstools.devices.specs import spectrograph_pressures, spectrograph_temperatures
from lvmopstools.devices.thermistors import read_thermistors
from lvmopstools.retrier import Retrier
from sdsstools.utils import GatheringTaskGroup

from lvmcryo.config import ValveConfig, get_internal_config
from lvmcryo.handlers.thermistor import ThermistorMonitor
from lvmcryo.handlers.valve import ValveHandler
from lvmcryo.tools import (
    TimerProgressBar,
    cancel_task,
    get_fake_logger,
    ln2_estops,
    o2_alert,
)


def convert_datetime_to_iso_8601_with_z_suffix(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_now():
    """Returns a UTC datetime for now."""

    return datetime.datetime.now(datetime.UTC)


class EventDict(BaseModel):
    """Dictionary of events."""

    start_time: datetime.datetime = field(default_factory=get_now)
    end_time: datetime.datetime | None = None
    purge_start: datetime.datetime | None = None
    purge_complete: datetime.datetime | None = None
    fill_start: datetime.datetime | None = None
    fill_complete: datetime.datetime | None = None
    fail_time: datetime.datetime | None = None
    abort_time: datetime.datetime | None = None

    @field_serializer("*")
    def serialize_dates(self, value: datetime.datetime | None) -> str | None:
        if value is None:
            return None
        return convert_datetime_to_iso_8601_with_z_suffix(value)


def get_valve_info():
    """Returns the valve information from the configuration file."""

    internal_config = get_internal_config()

    return {
        valve: ValveConfig(**data)
        for valve, data in internal_config["valve_info"].items()
    }


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
    log
        The logger instance. A new logger is created if not provided.
    valve_info
        A dictionary of valve name to actor and outlet name. If not provided,
        the internal configuration will be used.
    dry_run
        Does not actually operate the valves.
    alerts_route
        The API route to query system alerts.

    """

    cameras: list[str]
    purge_valve: str = "purge"
    interactive: bool = False
    log: logging.Logger = field(default_factory=get_fake_logger)
    valve_info: dict[str, ValveConfig] = field(default_factory=get_valve_info)
    dry_run: bool = False
    alerts_route: str | None = "http://lvm-hub.lco.cl:8090/api/alerts"

    def __post_init__(self):
        if self.interactive:
            console = getattr(self.log, "rich_console", None)
            self._progress_bar = TimerProgressBar(console)
            self.console = self._progress_bar.console
        else:
            self._progress_bar = None
            self.console = Console()

        self.valve_handlers: dict[str, ValveHandler] = {}
        for camera in self.cameras + [self.purge_valve]:
            if camera not in self.valve_info:
                raise ValueError(f"Cannot find valve info for {camera!r}.")

            actor = self.valve_info[camera].actor
            outlet = self.valve_info[camera].outlet
            thermistor = self.valve_info[camera].thermistor

            self.valve_handlers[camera] = ValveHandler(
                camera,
                actor,
                outlet,
                thermistor_info=thermistor.model_dump() if thermistor else None,
                progress_bar=self._progress_bar,
                log=self.log,
                dry_run=self.dry_run,
            )

        self._alerts_monitor_task: asyncio.Task | None = asyncio.create_task(
            self.monitor_alerts()
        )

        self.event_times = EventDict()

        self.failed: bool = False
        self.aborted: bool = False
        self.error: str | None = None

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

        retrier = Retrier(max_attempts=3, delay=1)

        # Check O2 alarms.
        if not self.alerts_route:
            self.log.warning("No alerts route provided. Not checking O2 alarms.")
        else:
            try:
                self.log.info("Checking for O2 alarms ...")
                if await o2_alert(self.alerts_route):
                    self.fail("O2 alarm detected.")
                else:
                    self.log.debug("No O2 alarms reported.")
            except Exception as err:
                self.fail(f"Error checking O2 alarms: {err}")

        # Check connection to the NPS outlets
        try:
            self.log.info("Checking connection to NPS outlets ...")
            for valve in self.valve_handlers:
                if not await self.valve_handlers[valve].check():
                    raise RuntimeError(f"valve {valve!r} failed or did not reply.")
        except Exception as err:
            self.fail(f"Failed checking connection to NPS outlets: {err}")

        if max_temperature is not None:
            self.log.info("Checking LN2 temperatures ...")
            try:
                spec_temperatures = await retrier(spectrograph_temperatures)()
            except Exception as err:
                self.fail(f"Failed reading spectrograph temperatures: {err}")
            else:
                for camera in self.cameras:
                    ln2_temp = spec_temperatures[f"{camera}_ln2"]

                    if ln2_temp is None:
                        self.fail(f"Failed retrieving {camera!r} temperature.")

                    if ln2_temp > max_temperature:
                        self.fail(
                            f"LN2 temperature for camera {camera!r} is "
                            f"{ln2_temp:.1f} C which is above the maximum allowed "
                            f"temperature ({max_temperature:.1f} C)."
                        )

        if max_pressure is not None:
            self.log.info("Checking pressures ...")
            try:
                spec_pressures = await retrier(spectrograph_pressures)()
            except Exception as err:
                self.fail(f"Failed reading spectrograph pressures: {err}")
            else:
                for camera in self.cameras:
                    pressure = spec_pressures[camera]

                    if pressure is None:
                        self.fail(f"Failed retrieving {camera!r} pressure.")

                    if pressure > max_pressure:
                        self.fail(
                            f"Pressure for camera {camera!r} is {pressure} Torr "
                            f"which is above the maximum allowed pressure "
                            f"({max_pressure} Torr)."
                        )

        if check_thermistors:
            self.log.info("Checking thermistors ...")

            try:
                thermistors = await read_thermistors()
            except Exception as err:
                self.fail(f"Failed reading thermistors: {err}")

            for valve in self.valve_handlers:
                thermistor_info = self.valve_info[valve].thermistor

                if thermistor_info is None:
                    self.log.warning(f"Cannot check thermistor for {valve!r}.")
                    continue

                if thermistor_info.disabled:
                    self.log.warning(f"Thermistor for {valve!r} is disabled.")
                    continue

                if thermistor_info.channel is None:
                    self.fail(f"Thermistor channel for {valve!r} not defined.")

                thermistor_value = thermistors[thermistor_info.channel]
                if thermistor_value is True:
                    self.fail(f"Thermistor for valve {valve!r} is active.")

        self.log.info("All pre-fill checks passed.")

        return True

    async def purge(
        self,
        purge_valve: str | None = None,
        use_thermistor: bool = True,
        min_purge_time: float | None = None,
        max_purge_time: float | None = None,
        prompt: bool | None = None,
        preopen_cb: Callable[[], Coroutine | Any] | None = None,
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
            ``use_thermistor=False`` this is effectively the purge time unless
            ``prompt=True`` and the purge is cancelled before reaching the
            timeout.
        prompt
            Whether to show a prompt to stop or cancel the purge. If ``None``,
            determined from the instance ``interactive`` attribute.
        preopen_cb
            A callback to run before opening the purge valve.

        """

        if purge_valve is None:
            purge_valve = self.purge_valve

        valve_handler = self.valve_handlers[purge_valve]

        self.event_times.purge_start = get_now()

        self.log.info(
            f"Beginning purge using valve {valve_handler.valve!r} with "
            f"use_thermistor={use_thermistor}, min_open_time={min_purge_time}, "
            f"max_purge_time={max_purge_time}."
        )

        prompt = prompt if prompt is not None else self.interactive
        if prompt:
            self._kb_monitor(action="purge")

        try:
            if preopen_cb:
                if asyncio.iscoroutinefunction(preopen_cb):
                    await preopen_cb()
                else:
                    preopen_cb()

            await valve_handler.start_fill(
                min_open_time=min_purge_time or 0.0,
                max_open_time=max_purge_time,
                use_thermistor=use_thermistor,
            )

            if not self.aborted:
                self.log.info("Purge complete.")

        except Exception:
            self.fail()
            raise

        finally:
            self.event_times.purge_complete = get_now()
            if prompt:
                sshkeyboard.stop_listening()
                await asyncio.sleep(1)

            thermistor_monitor = ThermistorMonitor()  # Singleton
            thermistor_monitor.stop()

    async def fill(
        self,
        cameras: list[str] | None = None,
        use_thermistors: bool = True,
        require_all_thermistors: bool = False,
        min_fill_time: float | None = None,
        max_fill_time: float | None = None,
        prompt: bool | None = None,
        preopen_cb: Callable[[], Coroutine | Any] | None = None,
    ):
        """Fills the selected cameras.

        Parameters
        ----------
        cameras
            The list of cameras to fill. If not provided, defaults to the values
            used to instantiate the `.LN2Handler` instance.
        use_thermistors
            Whether to use the thermistors to close the valves.
        require_all_thermistors
            Whether to require all thermistors to be active before closing the
            valves. If `False`, valves as closed as their thermistors become active.
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
        preopen_cb
            A callback to run before opening the fill valves.

        """

        cameras = cameras or self.cameras
        if cameras is None or len(cameras) == 0:
            self.fail("No cameras selected for filling.")

        fill_tasks: list[Coroutine] = []

        for camera in cameras:
            try:
                valve_handler = self.valve_handlers[camera]
            except KeyError:
                self.fail(f"Unable to find valve for camera {camera!r}.")

            fill_tasks.append(
                valve_handler.start_fill(
                    min_open_time=min_fill_time or 0.0,
                    max_open_time=max_fill_time,
                    use_thermistor=use_thermistors,
                    close_on_active=not require_all_thermistors,
                )
            )

        self.event_times.fill_start = get_now()

        self.log.info(
            f"Beginning fill on cameras {cameras!r} with "
            f"use_thermistors={use_thermistors}, min_open_time={min_fill_time}, "
            f"max_fill_time={max_fill_time}."
        )

        prompt = prompt if prompt is not None else self.interactive
        if prompt:
            self._kb_monitor(action="fill")

        try:
            if preopen_cb:
                if asyncio.iscoroutinefunction(preopen_cb):
                    await preopen_cb()
                else:
                    preopen_cb()

            await asyncio.gather(*fill_tasks)

            if not self.aborted:
                self.log.info("Fill complete.")

        except Exception:
            self.fail()
            raise

        finally:
            if use_thermistors and require_all_thermistors:
                # If we are waiting for all thermistors to be active,
                # we need to close all valves now.
                self.log.info("Closing all valves.")
                async with GatheringTaskGroup() as group:
                    for valve_handler in self.valve_handlers.values():
                        group.create_task(valve_handler._set_state(False))

            self.event_times.fill_complete = get_now()
            if prompt:
                sshkeyboard.stop_listening()
                await asyncio.sleep(1)

            thermistor_monitor = ThermistorMonitor()  # Singleton
            thermistor_monitor.stop()

    def get_valve_times(
        self,
        as_string: bool = False,
    ) -> dict[str, dict[str, str | datetime.datetime | bool | None]]:
        """Returns a dictionary of open/close times for the valves."""

        result: dict[str, dict[str, str | datetime.datetime | bool | None]] = {}

        for valve in self.valve_handlers:
            result[valve] = {
                "open_time": None,
                "close_time": None,
                "timed_out": False,
                "thermistor_first_active": None,
            }

            open_time = self.valve_handlers[valve].open_time
            close_time = self.valve_handlers[valve].close_time

            if open_time is not None:
                result[valve]["open_time"] = (
                    open_time.isoformat() if as_string else open_time
                )

            if close_time is not None:
                result[valve]["close_time"] = (
                    close_time.isoformat() if as_string else close_time
                )

            thermistor = self.valve_handlers[valve].thermistor
            if thermistor is not None and thermistor.first_active is not None:
                result[valve]["thermistor_first_active"] = (
                    thermistor.first_active.isoformat()
                    if as_string
                    else thermistor.first_active
                )

            result[valve]["timed_out"] = self.valve_handlers[valve].timed_out

        return result

    def _kb_monitor(self, action: str = "fill"):
        """Monitors the keyboard and cancels/aborts the fill.."""

        async def monitor_keys(key: str):
            """Parses a pressed key and cancels/aborts the fills."""

            if key not in ["x", "X", "enter"]:
                return

            if key == "x" or key == "X":
                self.log.warning("Aborting now.")
                # No not raise an alert here.
                await self.abort(
                    error="Aborted by user.",
                    close_valves=True,
                    raise_error=False,
                )

            elif key == "enter":
                self.log.warning("Finishing purge/fill.")
                await self.stop(only_active=True)

        self.console.print(
            Panel(
                Align(
                    render(
                        f'Press [green]"enter"[/] to finish the {action} '
                        'or [green]"x"[/] to abort.'
                    ),
                    "center",
                ),
                box=box.HEAVY,
                border_style="green",
            )
        )

        # No need to store this task. It will be automatically done when
        # sshkeyboard.stop_listening() is called.
        asyncio.create_task(
            sshkeyboard.listen_keyboard_manual(
                on_press=monitor_keys,
                sleep=0.1,
            )
        )

    async def monitor_alerts(self):
        """Monitors the system alerts and aborts the fill if necessary."""

        if not self.alerts_route:
            self.log.warning("No alerts route provided. Not monitoring alerts.")
            return

        n_failed: int = 0

        while True:
            try:
                if await o2_alert(self.alerts_route):
                    await self.abort(
                        error="O2 alarm detected: closing valves and aborting.",
                        close_valves=True,
                        raise_error=False,
                    )

                n_failed = 0

            except Exception as ee:
                self.log.warning(f"Error reading alerts: {ee}")
                n_failed += 1

            try:
                ln2_estops_active = await ln2_estops()
                if ln2_estops_active:
                    await self.abort(
                        error="LN2 estops detected: aborting.",
                        close_valves=False,  # The NPS will be unavailable
                        raise_error=False,
                    )
            except Exception:
                # Log the error but do not increase n_failed. The e-stops are a
                # low-level safety feature. We only monitor them here to cleanly
                # abort the fill if they are pressed, but worst case the code
                # will fail but that won't have an impact on the system.
                self.log.warning("Error reading LN2 estops.")

            if n_failed >= 10:
                await self.abort(
                    error="Too many errors reading alerts. Aborting.",
                    close_valves=True,
                    raise_error=False,
                )

            await asyncio.sleep(3)

    async def stop(self, close_valves: bool = True, only_active: bool = True):
        """Cancels ongoing fills and closes the valves.

        If ``only_active=True`` only active valves will be closed. Otherwise
        closes all valves.

        """

        tasks: list[Coroutine] = []

        for valve_handler in self.valve_handlers.values():
            if valve_handler.active or not only_active:
                tasks.append(valve_handler.finish(close_valve=close_valves))

        # Try to close as many valves as possible but then raise the error if
        # any failed.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                raise result

    async def clear(self):
        """Cleanly finishes tasks and other clean-up tasks."""

        sshkeyboard.stop_listening()

        if self._progress_bar is not None:
            self._progress_bar.close()

        self._alerts_monitor_task = await cancel_task(self._alerts_monitor_task)

    @overload
    def fail(self, error: str, raise_error: bool = True) -> NoReturn: ...

    @overload
    def fail(self, error=None, raise_error: bool = True) -> None: ...

    def fail(
        self,
        error: str | None = None,
        raise_error: bool = True,
    ) -> NoReturn | None:
        """Sets the fail flag and event time."""

        self.failed = True
        self.event_times.fail_time = get_now()

        if error:
            self.error = error
            self.log.error(error)
            if raise_error:
                raise RuntimeError(error)

    @overload
    async def abort(
        self,
        error: str | None,
        close_valves: bool,
        raise_error: bool = True,
    ) -> NoReturn: ...

    @overload
    async def abort(
        self,
        error: str | None,
        close_valves: bool,
        raise_error: Literal[True] = True,
    ) -> NoReturn: ...

    @overload
    async def abort(
        self,
        error: str | None,
        close_valves: bool,
        raise_error: Literal[False] = False,
    ) -> None: ...

    async def abort(
        self,
        error: str | None = None,
        close_valves: bool = False,
        raise_error: bool = True,
    ) -> NoReturn | None:
        """Aborts the fill."""

        self.aborted = True
        self.event_times.abort_time = get_now()

        # For each valve, close it (optionally) and finish the monitoring.
        await self.stop(close_valves=close_valves, only_active=False)

        self.fail(error=error or "Aborted.", raise_error=raise_error)
