#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-06-01
# @Filename: ion.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from drift import Drift
from drift.convert import data_to_float32

from ln2fill import config


def convert_pressure(volts: float):
    """Converts differential voltage to pressure in Torr."""

    # The calibration is a linear fit of the form y = mx + b
    m = 2.04545
    b = -6.86373

    log10_pp0 = m * volts + b  # log10(PPa), pressure in Pascal

    torr = 10**log10_pp0 * 0.00750062

    return torr


async def read_ion_pump(camera: str):
    """Reads the signal and on/off status from an ion pump."""

    config_ion = config["ion"][camera]

    drift = Drift(config_ion["host"], config_ion["port"])

    async with drift:
        signal_address = config_ion["signal_address"]
        onoff_address = config_ion["onoff_address"]

        signal = await drift.client.read_input_registers(signal_address, 2)
        onoff = await drift.client.read_input_registers(onoff_address, 1)

    diff_volt = data_to_float32(tuple(signal.registers))
    pressure = convert_pressure(diff_volt)

    onoff_status = bool(onoff.registers[0])

    return {"pressure": pressure, "is_on": onoff_status}
