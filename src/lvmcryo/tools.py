#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import time
from contextlib import suppress
from functools import partial
from logging import getLogger

from typing import TYPE_CHECKING, Any

from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TaskID, TextColumn

from lvmopstools.clu import CluClient

from lvmcryo.config import get_internal_config


if TYPE_CHECKING:
    from sdsstools.configuration import Configuration


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


async def close_all_valves(config: Configuration | None = None):
    """Closes all the outlets."""

    config = config or get_internal_config()
    valve_info = config["valves"]

    await asyncio.gather(
        *[
            valve_on_off(valve_info["actor"], valve_info["outlet"], False)
            for valve in valve_info
        ]
    )


def is_container():
    """Returns `True` if the code is running inside a container."""

    is_container = os.getenv("IS_CONTAINER", None)
    if not is_container or is_container in ["", "0"]:
        return False

    return True


class TimerProgressBar:
    """A progress bar with a timer."""

    def __init__(self, console: Console | None = None):
        self.progress = Progress(
            TextColumn("[yellow]({task.fields[label]})"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TextColumn("s"),
            expand=True,
            transient=False,
            auto_refresh=True,
            console=console,  # Need to use same console as logger.
        )
        self.console = self.progress.console

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
            f"[blue] {initial_description} ",
            total=int(max_time),
            label=label,
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

        if clear:
            self.progress.update(task_id, visible=False)

        await asyncio.sleep(1)  # Extra time for renders.


async def cancel_task(task: asyncio.Future | None):
    """Safely cancels a task."""

    if task is None or task.done():
        return None

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    return None


def render_template(
    template: str | None = None,
    file: str | os.PathLike | None = None,
    render_data: dict[str, Any] = {},
):
    """Renders a template using Jinja2.

    Parameters
    ----------
    template
        The template to render. If `None`, `file` must be defined.
    file
        The path to the file to render. If `None`, `template` must be defined.
    render_data
        The data to pass to the template.

    """

    if template is not None and file is not None:
        raise ValueError("Only one of template or file can be defined.")

    if template is not None:
        templates_path = pathlib.Path(__file__).parent / "templates"
        loader = FileSystemLoader(templates_path)
    elif file is not None:
        dirname = pathlib.Path(file).parent
        loader = FileSystemLoader(dirname)
        template = dirname.name
    else:
        raise ValueError("Either template or file must be defined.")

    env = Environment(
        loader=loader,
        lstrip_blocks=True,
        trim_blocks=True,
    )
    html_template = env.get_template(template)

    return html_template.render(**render_data)


class LockExistsError(RuntimeError):
    """Raised when a lock file already exists."""

    pass


@contextlib.contextmanager
def ensure_lock(lockfile: str | os.PathLike | pathlib.Path):
    """Ensures a lock file is created and deleted on exit.

    Raises an exception if the lock file already exists.

    """

    lockfile = pathlib.Path(lockfile)

    if lockfile.exists():
        raise LockExistsError(f"Lock file {lockfile} already exists.")

    lockfile.touch()

    try:
        yield
    finally:
        lockfile.unlink()


def get_fake_logger():
    """Gets a logger with a disabled handler."""

    logger = getLogger(__name__)
    logger.disabled = True

    return logger
