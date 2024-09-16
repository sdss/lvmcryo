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

from typing import TYPE_CHECKING, Any, Optional

from rich.progress import TaskID

from lvmopstools.clu import CluClient

from lvmcryo.config import get_internal_config
from lvmcryo.handlers.thermistor import ThermistorHandler
from lvmcryo.tools import (
    cancel_task,
    get_fake_logger,
)


if TYPE_CHECKING:
    from sdsstools.configuration import Configuration


__all__ = [
    "ValveHandler",
    "valve_on_off",
    "cancel_nps_threads",
    "close_all_valves",
    "outlet_info",
]


async def outlet_info(actor: str, outlet: str) -> dict[str, Any]:
    """Retrieves outlet information from the NPS."""

    async with CluClient() as client:
        cmd = await client.send_command(actor, f"status {outlet}")
        if cmd.status.did_fail:
            raise RuntimeError(f"Command '{actor} status {outlet}' failed.")

    return cmd.replies.get("outlet_info")


async def valve_on_off(
    actor: str,
    outlet_name: str,
    on: bool,
    timeout: float | None = None,
    use_script: bool = True,
    dry_run: bool = False,
) -> int | None:
    """Turns a valve on/off.

    Parameters
    ----------
    actor
        The NPS actor that commands the valve.
    outlet_name
        The name of the outlet to which the valve is connected.
    on
        Whether to turn the valve on or off.
    timeout
        Time, in seconds, after which the valve will be turned off.
    use_script
        If ``timeout`` is defined, the user script ``cycle_with_timeout``
        (must be defined in the NPS) to set a timeout after which the valve
        will be turned off. With ``use_script=False``, a ``timeout`` will
        block until the timeout is reached.
    dry_run
        Does not send the command to the NPS.

    Returns
    -------
    thread_id
        If ``use_script=True``, the thread number of the user script that
        was started. Otherwise `None`.

    """

    is_script: bool = False

    if on is True and isinstance(timeout, (int, float)) and use_script is True:
        # First we need to get the outlet number.
        info = await outlet_info(actor, outlet_name)
        id_ = info["id"]

        command_string = f"scripts run cycle_with_timeout {id_} {timeout}"
        is_script = True

    else:
        if on is False or timeout is None:
            command_string = f"{'on' if on else 'off'} {outlet_name}"
        else:
            command_string = f"on --off-after {timeout} {outlet_name}"

    if dry_run:
        return 0 if is_script else None

    async with CluClient() as client:
        command = await client.send_command(actor, command_string)
        if command.status.did_fail:
            raise RuntimeError(f"Command '{actor} {command_string}' failed")

    if is_script:
        script_data = command.replies.get("script")
        return script_data["thread_id"]

    return


async def cancel_nps_threads(actor: str, thread_id: int | None = None):
    """Cancels a script thread in an NPS.

    Parameters
    ----------
    actor
        The name of the NPS actor to command.
    thread_id
        The thread ID to cancel. If `None`, all threads in the NPS will be cancelled.

    """

    command_string = f"scripts stop {thread_id if thread_id else ''}"

    async with CluClient() as client:
        await client.send_command(actor, command_string)


async def close_all_valves(config: Configuration | None = None, dry_run: bool = False):
    """Closes all the outlets."""

    config = config or get_internal_config()
    valve_info = config["valves"]

    await asyncio.gather(
        *[
            valve_on_off(
                valve_info[valve]["actor"],
                valve_info[valve]["outlet"],
                False,
                dry_run=dry_run,
            )
            for valve in valve_info
        ]
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
    dry_run
        Does not actually operate the valves.

    """

    valve: str
    actor: str
    outlet: str
    thermistor_channel: str | None = None
    progress_bar: Optional[TimerProgressBar] = None
    log: logging.Logger = field(default_factory=get_fake_logger)
    dry_run: bool = False

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
        max_open_time: float | None = None,
        use_thermistor: bool = True,
    ):
        """Starts a fill.

        Parameters
        ----------
        min_open_time
            Minimum time to keep the valve open.
        max_open_time
            The maximumtime to keep the valve open. If ``None``, defaults to
            the ``max_open_time`` value.
        use_thermistor
            Whether to use the thermistor to close the valve. If ``True`` and
            ``fill_time`` is not ``None``, ``fill_time`` become the maximum
            open time.

        """

        if max_open_time is None:
            # Some hardcoded hard limits.
            if self.valve == "purge":
                max_open_time = 2000
            else:
                max_open_time = 600

            self.log.warning(
                f"No max_open_time provided for valve {self.valve!r}. "
                f"Using default of {max_open_time} seconds."
            )

        await self._set_state(True, timeout=max_open_time, use_script=True)

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
                max_open_time,
                label=self.valve,
                initial_description=initial_description,
                complete_description=complete_description,
            )

        await asyncio.sleep(2)
        self._timeout_task = asyncio.create_task(self._schedule_timeout(max_open_time))

        self.active = True

        self.event.clear()
        await self.event.wait()

    async def _schedule_timeout(self, timeout: float):
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
        if self._thread_id is not None and not self.dry_run:
            await cancel_nps_threads(self.actor, self._thread_id)

        thread_id = await valve_on_off(
            self.actor,
            self.valve,
            on,
            timeout=timeout,
            use_script=use_script,
            dry_run=self.dry_run,
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
                self.log.info(f"Valve {self.valve!r} was opened.")
        else:
            self.log.info(f"Valve {self.valve!r} was closed.")
