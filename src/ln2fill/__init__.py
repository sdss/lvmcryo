#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2023-11-10
# @Filename: __init__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib

from sdsstools import get_config, get_logger, get_package_version


# pip package name
NAME = "lvm-ln2fill"


config = get_config(
    "",
    config_file=pathlib.Path(__file__).parent / "config.yaml",
    allow_user=False,
)

log = get_logger(NAME, use_rich_handler=True)

# package name should be pip package name
__version__ = get_package_version(path=__file__, package_name=NAME)
