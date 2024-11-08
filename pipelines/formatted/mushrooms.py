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
    input_path = args.dataset

    # Load the dataset from CSV file
    df = pd.read_csv(input_path, sep=";", index_col=False)

    # Randomly sample a fraction of the dataset if specified
    if args.frac != 0.0:
        df = df.sample(frac=args.frac)

    # Subscribe dataframe to the tracker
    df = tracker.subscribe(df)
    tracker.analyze_changes(df)

    # Drop unnecessary columns from the dataframe
    df = df.drop(
        [
            "does-bruise-or-bleed",
            "gill-attachment",
            "gill-color",
            "gill-spacing",
            "habitat",
            "has-ring",
            "ring-type",
            "spore-print-color",
            "stem-color",
            "stem-root",
            "stem-surface",
            "veil-color",
            "veil-type",
        ],
        axis=1,
    )
    tracker.analyze_changes(df)

    # Convert the class column to binary values (0/1)
    df["class"] = df["class"].replace({"e": "1", "p": "0"}).astype(int)
    tracker.analyze_changes(df)

    # Drop rows with missing values
    df = df.dropna()
    tracker.analyze_changes(df)

    # Replace categorical values in specified columns with numerical values
    df = df.replace(
        {
            "cap-color": {
                "n": "1",
                "b": "2",
                "g": "3",
                "r": "3",
                "p": "4",
                "u": "5",
                "e": "6",
                "w": "7",
                "y": "8",
                "l": "9",
                "o": "10",
                "k": "11",
            },
            "cap-shape": {
                "b": "1",
                "c": "2",
                "x": "3",
                "f": "4",
                "s": "5",
                "p": "6",
                "o": "7",
            },
            "cap-surface": {
                "i": "1",
                "g": "2",
                "y": "3",
                "s": "4",
                "h": "5",
                "l": "6",
                "k": "7",
                "t": "8",
                "w": "9",
                "e": "10",
            },
            "season": {"s": "1", "u": "2", "a": "3", "w": "4"},
        }
    ).astype(
        {
            "cap-color": int,
            "cap-shape": int,
            "cap-surface": int,
            "season": int,
        }
    )
    tracker.analyze_changes(df)

    return df
