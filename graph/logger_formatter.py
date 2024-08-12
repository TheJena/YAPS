#!/usr/bin/env python3
# coding: utf-8
#
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Copyright (C) 2024      Federico Motta            <federico.motta@unimore.it>
#                         Pasquale Leonardo Lazzaro <pas.lazzaro@stud.uniroma3.it>
#                         Marialaura Lazzaro        <mar.lazzaro1@stud.uniroma3.it>
# Copyright (C) 2022-2024 Luca Gregori              <luca.gregori@uniroma3.it>
# Copyright (C) 2021-2022 Luca Lauro                <luca.lauro@uniroma3.it>
#
# This file is part of a provenance capturing suite (originally named DPDS)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging


class CustomFormatter(logging.Formatter):
    """
    Custom log formatter that adds colors to log levels.

    Usage:
        formatter = CustomFormatter()
        handler.setFormatter(formatter)

    Colors:
        - DEBUG: White
        - INFO: Green
        - WARNING: Yellow
        - ERROR: Red
        - CRITICAL: Magenta
    """

    LEVEL_COLORS = {
        "DEBUG": "\033[37m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    RESET_COLOR = "\033[0m"

    def format(self, record) -> str:
        """
        Formats the log record with colorized log level.

        :param record: The log record to format.
        :return: The formatted log record as a string.
        """
        levelname = record.levelname
        if levelname in self.LEVEL_COLORS:
            levelname_color = (
                f"{self.LEVEL_COLORS[levelname]}{levelname}{self.RESET_COLOR}"
            )
            record.levelname = levelname_color
        return super(CustomFormatter, self).format(record)
