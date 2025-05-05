#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-10-08
# @Filename: validate.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import logging
import pathlib
from functools import partial

from typing import NoReturn

import polars

from sdsstools.logger import SDSSLogger

from lvmcryo.config import Config
from lvmcryo.handlers.ln2 import LN2Handler
from lvmcryo.tools import get_fake_logger


__all__ = ["validate_fill"]


def log_or_raise(
    log: logging.Logger | SDSSLogger | None,
    raise_on_error: bool,
    message: str,
    level: int = logging.INFO,
):
    """Logs a message or raises an exception.

    Parameters
    ----------
    log
        A logger to output messages to the user.
    message
        The message to log or raise.
    level
        The logging level to use if ``log`` is provided.
    raise_on_error
        If `True`, raises an exception if the message level is ``ERROR`` or higher.

    """

    log = log or get_fake_logger()

    if level >= logging.ERROR and raise_on_error:
        raise RuntimeError(message)

    log.log(level, message)


def validate_fill(
    ln2_handler: LN2Handler,
    config: Config,
    log: logging.Logger | SDSSLogger | None = None,
    raise_on_error: bool = False,
) -> tuple[bool, str | None] | NoReturn:
    """Validates post-fill data.

    Parameters
    ----------
    ln2_handler
        The LN2 handler.
    data
        A dataframe with the data to validate. Usually the output of
        `.runner.post_fill`.
    log
        A logger to output messages to the user.
    raise_on_error
        If `True`, raises an exception if the validation fails. Otherwise the
        validation errors are logged and the result is returned as a boolean.

    Returns
    -------
    result
        A tuple in which the first element is a boolean indicating whether the
        validation failed, and the second element is a message with the validation
        result.

    """

    data = config.data_path
    if data is None:
        return (True, "No data path was provided.")

    max_temperature_increase = config.max_temperature_increase or 0.0

    log_p = partial(log_or_raise, log, raise_on_error)
    log_p("Validating post-fill data.")

    if isinstance(data, (pathlib.Path, str)):
        file_ = pathlib.Path(data)
        if not file_.exists():
            log_p(
                f"File {file_} does not exist. Cannot validate fill.",
                level=logging.WARNING,
            )
            return (False, None)

        data = polars.read_parquet(data)

    event_times = ln2_handler.event_times

    failed: bool = False
    error: str | None = None

    if data.height == 0:
        log_p("No post-fill data was collected.", level=logging.ERROR)
        return (False, "No post-fill data was collected.")

    # Make sure the time is UTC.
    data = data.with_columns(polars.col.time.dt.replace_time_zone("UTC"))

    # 1. Check that the LN2 temperature after the fill is lower than before.
    # We only do this if we have collected enough data after the fill completes.

    if event_times.fill_start is None or event_times.fill_complete is None:
        # There was no LN2 fill.
        pass
    else:
        ln2_temp = data.select(polars.col.time, polars.col("^temp_[rbz][1-3]_ln2$"))
        ln2_temp = ln2_temp.sort(polars.col.time)

        # Check that the last point was taken at least 3 minutes after the fill.
        max_time = ln2_temp["time"].max()
        extra_time = (max_time - event_times.end_time).total_seconds()  # type: ignore
        if extra_time < 3 * 60:
            log_p(
                "Not enough data collected after the fill "
                "to check the LN2 temperature difference.",
                level=logging.WARNING,
            )
        else:
            for column in ln2_temp.columns:
                if column == "time":
                    continue

                camera = column.split("_")[1]
                if camera not in ln2_handler.cameras:
                    continue

                temp0 = ln2_temp[0, column]
                temp1 = ln2_temp[-1, column]

                # Check if the temperature increased after the fill. If the temperature
                # increased but withing the threshold, log a warning. Otherwise, fail
                # the validation.
                if temp1 > temp0:
                    msg = (
                        f"LN2 temperature for camera {camera} increased "
                        f"from {temp0:.2f} to {temp1:.2f} degC after the fill."
                    )

                    if temp1 > (temp0 + max_temperature_increase):
                        failed = True
                        log_p(msg, level=logging.ERROR)
                        break
                    else:
                        log_p(msg, level=logging.WARNING)

    return (failed, error)
