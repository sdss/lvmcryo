#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: conftest.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pytest

from clu.command import Command, CommandStatus

import lvmcryo.tools


@pytest.fixture()
def command_done():
    command = Command()
    command.status = CommandStatus.DONE

    yield command


@pytest.fixture()
def command_failed():
    command = Command()
    command.status = CommandStatus.FAILED

    yield command


@pytest.fixture()
async def mock_client_start(mocker):
    yield mocker.patch.object(
        lvmcryo.tools.AMQPClient,
        "start",
    )


@pytest.fixture()
async def mock_client_send_command(mock_client_start, mocker, command_done):
    yield mocker.patch.object(
        lvmcryo.tools.AMQPClient,
        "send_command",
        return_value=command_done,
    )
