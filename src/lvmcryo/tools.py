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
import json
import logging
import os
import pathlib
import time
from contextlib import suppress
from functools import partial
from logging import FileHandler, getLogger

from typing import TYPE_CHECKING, Any

import httpx
from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TaskID, TextColumn

from lvmopstools.clu import CluClient
from lvmopstools.retrier import Retrier
from sdsstools.logger import CustomJsonFormatter
from sdsstools.utils import run_in_executor


if TYPE_CHECKING:
    from datetime import datetime

    from lvmcryo.config import Config
    from lvmcryo.handlers.ln2 import LN2Handler


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


@Retrier(max_attempts=3, delay=0.5)
async def o2_alert(route: str = "http://lvm-hub.lco.cl:8090/api/alerts"):
    """Is there an active O2 alert?"""

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(route)

        if response.status_code != 200:
            raise RuntimeError(response.text)
        else:
            alerts = response.json()
            return alerts["o2_alert"]

    except Exception as ee:
        raise RuntimeError(f"Error reading alerts: {ee}")


@Retrier(max_attempts=3, delay=0.5)
async def ln2_estops():
    """Returns :obj:`True` if any of the LN2 emergency stops are active."""

    async with CluClient() as client:
        try:
            status = await client.send_command("lvmecp", "status")
            safety_labels = status.replies.get("safety_status_labels").split(",")
            return "E_STOP_LN2" in safety_labels
        except Exception as ee:
            raise RuntimeError(f"Error reading estops: {ee}")


def add_json_handler(log: logging.Logger, json_path: os.PathLike | pathlib.Path):
    """Adds a JSON handler to a logger."""

    json_handler = logging.FileHandler(str(json_path), mode="w")
    json_handler.setLevel(5)
    json_handler.setFormatter(CustomJsonFormatter())
    log.addHandler(json_handler)

    return json_handler


def date_json(date: datetime | None) -> str | None:
    """Serialises a datetime object to a JSON string."""

    return date.isoformat() if date else None


class DBHandler:
    """Handles writing the fill to the database.

    Parameters
    ----------
    action
        The action being performed.
    handler
        The `.LN2Handler` instance.
    config
        The configuration object.
    api_db_route
        The API route to write the data to the database.
    json_handler
        The logging handler used to write JSON data.

    """

    def __init__(
        self,
        action: str,
        handler: LN2Handler,
        config: Config,
        api_route: str | None = None,
        json_handler: FileHandler | None = None,
    ) -> None:
        self.pk: int | None = None

        self.action = action
        self.handler = handler
        self.config = config
        self.api_route = api_route or config.internal_config["api_routes.register_fill"]

        self.json_handler = json_handler

    def get_log_data(self):
        """Returns the log data for the fill."""

        if self.json_handler:
            self.json_handler.flush()
            json_path = pathlib.Path(self.json_handler.baseFilename)
            with json_path.open("r") as ff:
                return [json.loads(line) for line in ff.readlines()]

        return None

    async def write(
        self,
        complete: bool = False,
        plot_paths: dict[str, pathlib.Path] = {},
        error: Exception | str | None = None,
        raise_on_error: bool = False,
    ):
        """Records the fill to the database.

        Parameters
        ----------
        complete
            Whether the action is complete.
        plot_paths
            A dictionary with the paths to the plots.
        error
            The error message or ``None`` if no error.
        raise_on_error
            Whether to raise an exception if the write fails.

        Returns
        -------
        pk
            The primary key of the new record in the database.

        """

        log = self.handler.log
        event_times = self.handler.event_times
        log_path = self.config.log_path

        json_path = self.json_handler.baseFilename if self.json_handler else None
        json_file = str(json_path) if json_path and self.config.write_json else None

        configuration_json = self.config.model_dump() | {
            valve: valve_model.model_dump()
            for valve, valve_model in self.config.valve_info.items()
        }

        payload = {
            "action": self.action,
            "complete": complete,
            "pk": self.pk,
            "start_time": date_json(event_times.start_time),
            "end_time": date_json(event_times.end_time),
            "purge_start": date_json(event_times.purge_start),
            "purge_complete": date_json(event_times.purge_complete),
            "fill_start": date_json(event_times.fill_start),
            "fill_complete": date_json(event_times.fill_complete),
            "fail_time": date_json(event_times.fail_time),
            "abort_time": date_json(event_times.abort_time),
            "failed": self.handler.failed,
            "aborted": self.handler.aborted,
            "plot_paths": {k: str(v) for k, v in plot_paths.items()},
            "log_file": str(log_path) if log_path else None,
            "valve_times": self.handler.get_valve_times(as_string=True),
            "json_file": json_file,
            "log_data": self.get_log_data(),
            "configuration": configuration_json,
            "error": str(error) if error is not None else None,
        }

        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.post(self.api_route, json=payload)

            if response.status_code != 200:
                if raise_on_error:
                    raise RuntimeError(f"Error writing to the DB: {response.text}")
                else:
                    log.warning(f"Error writing to the DB: {response.text}")
                    log.warning(f"DB payload: {payload}")

                return self.pk

            self.pk = response.json()

        return self.pk
