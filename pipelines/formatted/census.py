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
    df = pd.read_csv(input_path)

    # Assign names to columns
    names = [
        "age",
        "workclass",
        "fnlwgt",
        "education",
        "education-num",
        "marital-status",
        "occupation",
        "relationship",
        "race",
        "sex",
        "capital-gain",
        "capital-loss",
        "hours-per-week",
        "native-country",
        "label",
    ]

    df.columns = names

    # Perform random sampling if specified
    if args.frac != 0.0:
        df = df.sample(frac=args.frac)

    # Subscribe dataframe to tracker
    df = tracker.subscribe(df)
    tracker.analyze_changes(df)

    # Strip whitespace from categorical columns and replace "?" with 0
    columns = [
        "education",
        "label",
        "marital-status",
        "native-country",
        "occupation",
        "race",
        "relationship",
        "sex",
        "workclass",
    ]
    df[columns] = df[columns].applymap(str.strip)
    df = df.replace("?", 0)
    tracker.analyze_changes(df)

    # One-hot encode education column
    columns = ["education"]
    for i, col in enumerate(columns):
        dummies = pd.get_dummies(df[col])
        df_dummies = dummies.add_prefix(col + "_")
        df = df.join(df_dummies)
        df = df.drop([col], axis=1)
    tracker.analyze_changes(df)

    # Replace sex and label columns with numerical values
    df = df.replace(
        {
            "sex": {"Male": 1, "Female": 0},
            "label": {"<=50K": 0, ">50K": 1},
        }
    )
    tracker.analyze_changes(df)

    # Drop fnlwgt column
    df = df.drop(["fnlwgt"], axis=1)
    tracker.analyze_changes(df)

    # Rename hours-per-week column to hw
    df = df.rename(columns={"hours-per-week": "hw"}, inplace=True)
    tracker.analyze_changes(df)

    return df
