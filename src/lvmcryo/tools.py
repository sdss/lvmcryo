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

from typing import Any

from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TaskID, TextColumn

from sdsstools.utils import run_in_executor


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
            refresh_per_second=1,
            expand=True,
            transient=False,
            auto_refresh=False,
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
                asyncio.create_task(run_in_executor(self.progress.refresh))

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

    def close(self):
        """Closes the progress bar."""

        self.progress.console.clear_live()
        self.progress.stop()


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
