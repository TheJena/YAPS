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

import pandas as pd


def run_pipeline(args, tracker) -> None:
    input_path = args.datasets

    df = pd.read_csv(input_path, sep=";", index_col=False)

    if args.frac != 0.0:
        df = df.sample(frac=args.frac)

    # Subscribe dataframe
    df = tracker.subscribe(df)

    df = df.drop(
        [
            "does-bruise-or-bleed",
            "gill-attachment",
            "gill-color",
            "stem-root",
            "stem-surface",
            "stem-color",
            "veil-type",
            "veil-color",
            "has-ring",
            "ring-type",
            "spore-print-color",
            "habitat",
            "gill-spacing",
        ],
        axis=1,
    )

    # Assign 1 if class is 'e', 0 otherwise
    df["class"] = df["class"].replace({"e": 1, "p": 0})

    df = df.dropna()

    df = df.replace(
        {
            "cap-color": {
                "n": 1,
                "b": 2,
                "g": 3,
                "r": 3,
                "p": 4,
                "u": 5,
                "e": 6,
                "w": 7,
                "y": 8,
                "l": 9,
                "o": 10,
                "k": 11,
            },
            "cap-shape": {
                "b": 1,
                "c": 2,
                "x": 3,
                "f": 4,
                "s": 5,
                "p": 6,
                "o": 7,
            },
            "cap-surface": {
                "i": 1,
                "g": 2,
                "y": 3,
                "s": 4,
                "h": 5,
                "l": 6,
                "k": 7,
                "t": 8,
                "w": 9,
                "e": 10,
            },
            "season": {"s": 1, "u": 2, "a": 3, "w": 4},
        }
    )
