#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: test_tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pytest

from ln2fill import config
from ln2fill.tools import close_all, valve_on_off, valves_on_off


async def test_valve_on_off(mock_client_send_command):
    assert (await valve_on_off("r1")) is True


async def test_valve_on_off_fails(mock_client_send_command, command_failed):
    mock_client_send_command.return_value = command_failed

    with pytest.raises(RuntimeError):
        await valve_on_off("r1")


async def test_valve_on_off_bad_valve():
    with pytest.raises(ValueError):
        await valve_on_off("a1")


async def test_valves_on_off(mocker):
    mocker.patch("ln2fill.tools.valve_on_off")

    valves: list[str] = list(config["valves"])

    operated: list[list[str]] = []
    async for op in valves_on_off(valves):
        operated.append(op)

    assert len(operated) == 4


async def test_close_all(mocker):
    mm = mocker.patch("ln2fill.tools.valves_on_off")

    valves: list[str] = list(config["valves"])

    await close_all()
    mm.assert_called_once()
    mm.assert_called_with(valves)
