#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: test_tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pytest
from pytest_mock import MockFixture

from ln2fill.tools import close_all_valves, valve_on_off


async def test_valve_on_off(mock_client_send_command):
    assert (await valve_on_off("r1", True)) is None


async def test_valve_on_off_fails(mock_client_send_command, command_failed):
    mock_client_send_command.return_value = command_failed

    with pytest.raises(RuntimeError):
        await valve_on_off("r1", True)


async def test_valve_on_off_bad_valve():
    with pytest.raises(ValueError):
        await valve_on_off("a1", True)


async def test_close_all_valves(mocker: MockFixture):
    mm = mocker.patch("ln2fill.tools.valve_on_off")

    await close_all_valves()
    mm.assert_called()
