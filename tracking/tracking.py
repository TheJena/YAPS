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

from typing import Dict
import pandas as pd


class ProvenanceTracker:
    def __init__(self, save_on_neo4j=False):
        self.save_on_neo4j = save_on_neo4j
        self.tracking_enabled = False
        self.changes: dict[int, dict[str, pd.DataFrame]] = {}
        self.operation_counter = 0

    def subscribe(self, df):
        self.df_before = df.copy()
        self.tracking_enabled = True
        return df

    """
    function that analyze changes before and after the operation
    the result will be a dictionary of dictionaries {operation_number:{"before":df_input, "after":df_output}}
    where operation number is the number of the operation/activity in chronological order of execution
    """

    def analyze_changes(self, df_after):
        if not self.tracking_enabled:
            return
        self.df_after = df_after.copy()
        self.changes[self.operation_counter] = {
            "before": self.df_before,
            "after": self.df_after,
        }
        self.df_before = self.df_after.copy()
        self.operation_counter += 1

    def get_changes(self):
        return self.changes
