#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import os
import re
import time
import warnings
from contextlib import suppress
from functools import partial

from typing import Any, overload

import asyncudp
import httpx
import pandas
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TaskID, TextColumn

from clu import AMQPClient

from . import config


class CluClient:
    """AMQP client asynchronous generator.

    Returns an object with an ``AMQPClient`` instance. The normal way to
    use it is to do ::

        async with CluClient() as client:
            await client.send_command(...)

    Alternatively one can do ::

        client = await anext(CluClient())
        await client.send_command(...)

    The asynchronous generator differs from the one in ``AMQPClient`` in that
    it does not close the connection on exit.

    This class is a singleton, which effectively means the AMQP client is reused
    during the life of the worker. The singleton can be cleared by calling
    `.clear`.

    """

    __initialised: bool = False
    __instance: CluClient | None = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(CluClient, cls).__new__(cls)
            cls.__instance.__initialised = False

        return cls.__instance

    def __init__(self):
        if self.__initialised is True:
            return

        host: str = os.environ.get("RABBITMQ_HOST", config["rabbitmq.host"])
        port: int = int(os.environ.get("RABBITMQ_PORT", config["rabbitmq.port"]))

        self.client = AMQPClient(host=host, port=port)
        self.__initialised = True

    async def __aenter__(self):
        if not self.client.is_connected():
            await self.client.start()

        return self.client

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __anext__(self):
        if not self.client.is_connected():
            await self.client.start()

        return self.client

    @classmethod
    def clear(cls):
        """Clears the current instance."""

        cls.__instance = None
        cls.__initialised = False


async def valve_info(valve: str) -> dict[str, Any]:
    """Retrieves valve information from the NPS."""

    actor = config[f"valves.{valve}.actor"]
    outlet = config[f"valves.{valve}.outlet"]

    async with CluClient() as client:
        cmd = await client.send_command(actor, f"status {outlet}")
        if cmd.status.did_fail:
            raise RuntimeError(f"Command '{actor} status {outlet}' failed.")

    return cmd.replies.get("outlet_info")


async def valve_on_off(
    valve: str,
    on: bool,
    timeout: float | None = None,
    use_script: bool = True,
) -> int | None:
    """Turns a valve on/off.

    Parameters
    ----------
    valve
        The name of the valve. Must be one of the valves defined in the
        configuration file.
    on
        Whether to turn the valve on or off.
    timeout
        Time, in seconds, after which the valve will be turned off.
    use_script
        If ``timeout`` is defined, the user script ``cycle_with_timeout``
        (must be defined in the NPS) to set a timeout after which the valve
        will be turned off. With ``use_script=False``, a ``timeout`` will
        block until the timeout is reached.

    Returns
    -------
    thread_id
        If ``use_script=True``, the thread number of the user script that
        was started. Otherwise `None`.

    """

    if valve not in config["valves"]:
        raise ValueError(f"Unknown valve {valve!r}.")

    actor = config[f"valves.{valve}.actor"]
    outlet = config[f"valves.{valve}.outlet"]

    is_script: bool = False

    if on is True and isinstance(timeout, (int, float)) and use_script is True:
        # First we need to get the outlet number.
        outlet_info = await valve_info(valve)
        id_ = outlet_info["id"]

        command_string = f"scripts run cycle_with_timeout {id_} {timeout}"

        is_script = True

    else:
        if on is False or timeout is None:
            command_string = f"{'on' if on else 'off'} {outlet}"
        else:
            command_string = f"on --off-after {timeout} {outlet}"

    # async with CluClient() as client:
    #     command = await client.send_command(actor, command_string)
    #     if command.status.did_fail:
    #         raise RuntimeError(f"Command '{actor} {command_string}' failed")

    if is_script:
        return 1
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


async def close_all_valves():
    """Closes all the outlets."""

    valves = list(config["valves"])
    await asyncio.gather(*[valve_on_off(valve, False) for valve in valves])


async def get_spectrograph_status(spectrographs: list[str] = ["sp1", "sp2", "sp3"]):
    """Returns pressures and CCD and LN2 temperatures for all cryostats."""

    api_url: str = config["api.url"]

    api_response: dict[str, float] = {}
    async with httpx.AsyncClient(base_url=api_url) as client:
        response_specs = await asyncio.gather(
            *[client.get(f"/spectrographs/{spec}/summary") for spec in spectrographs]
        )

        for rs in response_specs:
            if rs.status_code != 200:
                raise ValueError("Invalid response from API.")
            api_response.update(rs.json())

    response: dict[str, dict[str, float]] = {}
    for spec in spectrographs:
        for camera in ["r", "b", "z"]:
            cryostat = f"{camera}{spec[-1]}"
            response[cryostat] = {}
            for label in ["ln2", "pressure"]:
                cryostat_label = f"{cryostat}_{label}"
                if cryostat_label in api_response:
                    response[cryostat][label] = api_response[cryostat_label]
                else:
                    warnings.warn(f"Cannot find label {cryostat_label!r}.")

    return response


@overload
async def read_thermistors_influxdb(
    thermistor: str,
    interval: None,
) -> bool:
    ...


@overload
async def read_thermistors_influxdb(
    thermistor: None,
    interval: None,
) -> dict[str, bool]:
    ...


@overload
async def read_thermistors_influxdb(
    thermistor: str | None,
    interval: float,
) -> pandas.DataFrame:
    ...


async def read_thermistors_influxdb(
    thermistor: str | None = None,
    interval: float | None = None,
) -> bool | dict[str, bool] | pandas.DataFrame:
    """Reads the thermistors by querying InfluxDB over the API."""

    api_url: str = config["api.url"]

    if thermistor is not None:
        route = f"/spectrographs/thermistors/{thermistor}"
    else:
        route = "/spectrographs/thermistors"

    if interval is not None:
        route += f"?interval={interval}"

    async with httpx.AsyncClient(base_url=api_url) as client:
        response = await client.get(route)

        if response.status_code != 200:
            raise httpx.HTTPError(f"Invalid response from API: {response.status_code}")

    if interval is not None:
        df = pandas.DataFrame(**response.json())
        df["time"] = pandas.to_datetime(df["time"])
        return df

    return response.json()


async def read_thermistors() -> dict[str, bool]:
    """Reads the thermistors directly by connecting to the device.."""

    host = config["thermistors.host"]
    port = config["thermistors.port"]
    mapping = config["thermistors.mapping"]

    socket = await asyncio.wait_for(
        asyncudp.create_socket(remote_addr=(host, port)),
        timeout=5,
    )

    socket.sendto(b"$016\r\n")
    data, _ = await asyncio.wait_for(socket.recvfrom(), timeout=5)

    match = re.match(rb"!01([0-9A-F]+)\r", data)
    if match is None:
        raise ValueError(f"Invalid response from thermistor server at {host!r}.")

    value = int(match.group(1), 16)

    channels: dict[str, bool] = {}
    for channel in range(16):
        channel_name = mapping.get(f"channel{channel}", "")
        if channel_name == "":
            continue
        channels[channel_name] = bool((value & 1 << channel) > 0)

    return channels


def is_container():
    """Returns `True` if the code is running inside a container."""

    is_container = os.getenv("IS_CONTAINER", None)
    if not is_container or is_container in ["", "0"]:
        return False

    return True


class TimerProgressBar:
    """A progress bar with a timer."""

    def __init__(self, console: Console | None = None):
        self.console = console

        self.progress = Progress(
            TextColumn("[yellow]({task.fields[label]})"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TextColumn("s"),
            expand=True,
            transient=False,
            auto_refresh=True,
            console=self.console,  # Need to use same console as logger.
        )

        self._tasks: dict[TaskID, asyncio.Task] = {}

    async def add_timer(
        self,
        max_time: float,
        label: str = "",
        initial_description: str = "Fill in progress ...",
        complete_description: str = "Fill complete",
    ):
        """Starts the timer."""

        task_id = self.progress.add_task(
            f"[blue] {initial_description} ", total=int(max_time), label=label
        )

        self.progress.start()

        start_time = time.time()

        async def update_timer():
            elapsed = 0
            while True:
                if elapsed > max_time:
                    break

                self.progress.update(task_id, advance=1, visible=True)

                await asyncio.sleep(1)
                elapsed += 1

        done_timer_partial = partial(
            self._done_timer,
            start_time,
            max_time,
            complete_description,
            task_id,
        )

        _task = asyncio.create_task(update_timer())
        _task.add_done_callback(done_timer_partial)

        self._tasks[task_id] = _task

        return task_id

    def _done_timer(
        self,
        start_time: float,
        max_time: float,
        complete_description: str,
        task_id: TaskID,
        *_,
    ):
        completed = int(time.time() - start_time)
        if completed > max_time:
            completed = int(max_time)

        self.progress.update(
            task_id,
            description=f"[green] {complete_description} ",
        )
        self.progress.refresh()

    async def stop_timer(self, task_id: TaskID, clear: bool = False):
        """Cancels a timer."""

        if task_id not in self._tasks:
            raise ValueError("Task ID not found.")

        _task = self._tasks[task_id]
        await cancel_task(_task)

        del self._tasks[task_id]

        # self._done_timer()
        if clear:
            self.progress.update(task_id, visible=False)

        await asyncio.sleep(1)  # Extra time for renders.


async def cancel_task(task: asyncio.Future | None):
    """Safely cancels a task."""

    if task is None or task.done():
        return

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
