#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import os
import re
import warnings
from collections import defaultdict

from typing import Sequence

import asyncudp
import httpx

from clu import AMQPClient

from . import config


def get_client(host: str | None = None):
    """Get an AMQP client to the RabbitMQ exchange."""

    return AMQPClient(host=host or str(config["rabbitmq.host"]))


async def valve_on_off(
    valve: str,
    on: bool = True,
    off_after: int | None = None,
    client: AMQPClient | None = None,
):
    """Turns a valve on/off."""

    if valve not in config["valves"]:
        raise ValueError(f"Unknown valve {valve!r}.")

    client = client or get_client()

    async with client:
        actor = config[f"valves.{valve}.actor"]
        outlet = config[f"valves.{valve}.outlet"]

        if off_after is None:
            command_string = f"{'on' if on else 'off'} {outlet}"
        else:
            if on is False:
                raise ValueError("off_after requires on=True.")
            command_string = f"on --off-after {off_after} {outlet}"

        command = await client.send_command(actor, command_string)
        if command.status.did_fail:
            raise RuntimeError(f"Command {actor} {command_string} failed")

        return True


async def valves_on_off(valves: Sequence[str], on: bool = True):
    """Turns on/off multiple valves as fast as possible."""

    client = get_client()

    actor_to_valves = defaultdict(list)
    for valve in valves:
        actor_to_valves[config[f"valves.{valve}.actor"]].append(valve)

    nn = max(map(len, actor_to_valves.values()))

    for ii in range(nn):
        coros = []
        valves = []
        for vv in actor_to_valves.values():
            if len(vv) > ii:
                coros.append(valve_on_off(vv[ii], on=on, client=client))
                valves.append(vv[ii])
        await asyncio.gather(*coros)
        yield valves


async def close_all():
    """Closes all the outlets."""

    async for _ in valves_on_off(list(config["valves"])):
        pass


async def get_spectrograph_status(spectrographs: list[str] = ["sp1", "sp2", "sp3"]):
    """Returns pressures and CCD and LN2 temperatures for all cryostats."""

    api_url: str = config["api.url"]

    api_response: dict[str, float] = {}
    async with httpx.AsyncClient(base_url=api_url) as client:
        response_specs = await asyncio.gather(
            *[client.get(f"/spectrographs/{spec}/summary") for spec in spectrographs]
        )

        for rs in response_specs:
            if rs.status_code != 200:
                raise ValueError("Invalid response from API.")
            api_response.update(rs.json())

    response: dict[str, dict[str, float]] = {}
    for spec in spectrographs:
        for camera in ["r", "b", "z"]:
            cryostat = f"{camera}{spec[-1]}"
            response[cryostat] = {}
            for label in ["ln2", "pressure"]:
                cryostat_label = f"{cryostat}_{label}"
                if cryostat_label in api_response:
                    response[cryostat][label] = api_response[cryostat_label]
                else:
                    warnings.warn(f"Cannot find label {cryostat_label!r}.")

    return response


async def read_thermistors() -> dict[str, bool]:
    """Reads the thermistors."""

    host = config["thermistors.host"]
    port = config["thermistors.port"]
    mapping = config["thermistors.mapping"]

    socket = await asyncio.wait_for(
        asyncudp.create_socket(remote_addr=(host, port)),
        timeout=5,
    )

    socket.sendto(b"$016\r\n")
    data, _ = await asyncio.wait_for(socket.recvfrom(), timeout=5)

    match = re.match(rb"!01([0-9A-F]+)\r", data)
    if match is None:
        raise ValueError(f"Invalid response from thermistor server at {host!r}.")

    value = int(match.group(1), 16)

    channels: dict[str, bool] = {}
    for channel in range(16):
        channel_name = mapping.get(f"channel{channel}", "")
        if channel_name == "":
            continue
        channels[channel_name] = bool((value & 1 << channel) > 0)

    return channels


def is_container():
    """Returns `True` if the code is running inside a container."""

    is_container = os.getenv("IS_CONTAINER", None)
    if not is_container or is_container in ["", "0"]:
        return False

    return True
