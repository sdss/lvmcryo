#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
from collections import defaultdict

from typing import Sequence

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
