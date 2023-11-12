#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-11
# @Filename: ln2fill.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import logging

from sdsstools.logger import SDSSLogger, get_logger


class LN2Fill:
    """The main LN2 purge/fill class.

    Parameters
    ----------
    interactive
        Whether to show interactive features.
    log
        A logger instance. Must be an ``sdsstools.logger.SDSSLogger`` instance
        or `None`, in which case a new logger will be created.
    quiet
        If `True`, only outputs error messages.

    """

    def __init__(
        self,
        interactive: bool = True,
        log: SDSSLogger | None = None,
        quiet: bool = False,
    ):
        self.interactive = interactive

        self.log = log or get_logger("lvm-ln2fill", use_rich_handler=True)
        if quiet:
            self.log.sh.setLevel(logging.ERROR)
