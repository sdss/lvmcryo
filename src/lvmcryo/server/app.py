#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2026-01-14
# @Filename: app.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import base64
import logging
import os
from contextlib import asynccontextmanager

from typing import Annotated, AsyncIterator

from fastapi import FastAPI, Query

from lvmcryo.handlers.valve import close_all_valves
from lvmcryo.runner import clear_lock as clear_lock_helper
from lvmcryo.runner import ln2_runner
from lvmcryo.tools import lockfile_exists


logger = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield


app = FastAPI(swagger_ui_parameters={"tagsSorter": "alpha"}, lifespan=lifespan)


@app.get("/ping", summary="Health check endpoint.")
async def ping():
    return {"status": "ok"}


@app.get("/filling", summary="Check if LN2 fill operation is ongoing.")
async def filling():
    """Checks if an LN2 fill operation is ongoing."""

    return {"filling": lockfile_exists()}


@app.get("/manual-fill", summary="Starts a manual LN2 fill operation.")
async def manual_fill(
    password: Annotated[
        str,
        Query(
            description="The password to authorize manual LN2 fills.",
        ),
    ],
    clear_lock: Annotated[
        bool,
        Query(
            description="If true, clears any existing lock before starting the fill."
        ),
    ] = False,
):
    """Starts a manual LN2 purge and fill operation.

    The route returns immediately after starting the operation in the background.

    """

    password_b64 = os.environ.get("LVMCRYO_FILL_PASSWORD", None)

    if password_b64 is None:
        return {"result": False, "reason": "Fill password not available."}

    password_bytes = password_b64.encode("utf-8")
    password_pt = base64.b64decode(password_bytes).decode("utf-8")

    if password != password_pt:
        return {"result": False, "reason": "Invalid password."}

    tasks: list[asyncio.Task] = []

    if lockfile_exists():
        if not clear_lock:
            return {
                "result": False,
                "reason": "Lock file exists. LN2 fill already in process.",
            }

    tasks.append(
        ln2_runner(
            profile="manual-fill",
            clear_lock=clear_lock,
            dry_run=True,
            quiet=True,
        )
    )

    asyncio.gather(*tasks)

    return {"result": True}


@app.get("/abort", summary="Abort LN2 fill operations and releases the lock.")
async def abort_ln2_fill(
    wait: Annotated[
        bool,
        Query(
            description="Wait for the operation to complete. "
            "Otherwise returns immediately and runs in the background."
        ),
    ] = False,
):
    """Aborts any ongoing LN2 fill operation and releases the lock."""

    async def _abort():
        await close_all_valves()
        await clear_lock_helper(wait=True)

    abort_task = asyncio.create_task(_abort())

    if wait:
        await abort_task

    return {"result": True}
