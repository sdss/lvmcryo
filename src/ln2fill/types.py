#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: types.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from typing import Optional, TypedDict


class OptionsType(TypedDict):
    """CLI  options."""

    cameras: Optional[str]
    check_pressure: bool
    check_temperature: bool
    max_pressure: float
    min_pressure: float
    max_temperature: float
    min_temperature: float
    purge_time: Optional[float]
    min_purge_time: float
    max_purge_time: float
    fill_time: Optional[float]
    min_fill_time: float
    max_fill_time: float
    quiet: bool
    write_json: bool
    write_log: bool
    log_path: Optional[str]
    write_measurements: bool
    measurements_path: Optional[str]
    measurements_interval: float
    measurements_extra_time: float
    generate_qa: bool
    qa_path: Optional[str]
    slack: bool
    slack_route: Optional[str]
    email: bool
    email_recipients: Optional[str]
