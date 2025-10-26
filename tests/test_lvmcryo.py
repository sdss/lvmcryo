#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-09-16
# @Filename: test_lvmcryo.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from lvmcryo.handlers.ln2 import LN2Handler


async def test_l2n_handler():
    """Tests the LN2Handler class."""

    handler = LN2Handler(["r1", "purge"], monitor_alerts=False)
    assert handler is not None

    r1_valve = handler.valve_handlers["r1"]
    purge_valve = handler.valve_handlers["purge"]

    assert r1_valve is not None
    assert purge_valve is not None

    assert r1_valve.thermistor_info is not None
    assert purge_valve.thermistor_info is not None

    assert r1_valve.thermistor_info["monitoring_interval"] == 1.0
    assert r1_valve.thermistor_info["channel"] == "r1"

    assert purge_valve.thermistor_info["monitoring_interval"] == 1.0
    assert purge_valve.thermistor_info["channel"] == "supply"
