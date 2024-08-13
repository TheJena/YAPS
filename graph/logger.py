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
# This file is part of YAPS, a provenance capturing suite
#
# YAPS is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# YAPS is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
# License for more details.
#
# You should have received a copy of the GNU General Public License
# along with YAPS.  If not, see <https://www.gnu.org/licenses/>.

import logging

from graph.logger_formatter import CustomFormatter
from graph.decorators import Singleton


@Singleton
class CustomLogger(logging.Logger):
    """
    Custom logger class that provides a logger with a custom formatter.
    """

    def __init__(self, name, level=logging.DEBUG):
        """
        Initializes the CustomLogger.

        :param name: The name of the logger.
        :param level: The logging level (default is DEBUG).
        """
        super().__init__(name)
        self.setLevel(level)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        formatter = CustomFormatter(
            "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
        )

        console_handler.setFormatter(formatter)
        self.addHandler(console_handler)

    def set_level(self, level):
        """
        Sets the logging level for the logger.

        :param level: The new logging level to be set.
        """
        self.setLevel(level)
        for handler in self.handlers:
            handler.setLevel(level)
