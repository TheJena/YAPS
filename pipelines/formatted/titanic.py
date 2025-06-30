#!/usr/bin/env python3
# coding: utf-8
#
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Copyright (C) 2024-2025 Federico Motta            <federico.motta@unimore.it>
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

    # Load data from CSV file into a Pandas DataFrame
    df = pd.read_csv(input_path, header=0)

    # Randomly sample a fraction of the data if specified
    if args.frac != 0.0:
        df = df.sample(frac=args.frac)

    # Subscribe dataframe to tracker for provenance analysis
    df = tracker.subscribe(df)
    tracker.analyze_changes(df)

    # Drop unnecessary columns from the DataFrame
    cols = ["Name", "Ticket", "Cabin"]
    df = df.drop(cols, axis=1)
    tracker.analyze_changes(df)

    # Remove rows with missing values from the DataFrame
    df = df.dropna()  # sum(df["Age"].isna())==177, sum(df["Embarked"].isna())==2
    tracker.analyze_changes(df)

    # One-hot encode categorical variables and drop original columns
    cols = ["Pclass", "Sex", "Embarked"]
    # tracker.dataframe_tracking = False  # uncomment to have the missing link (?)
    for i, col in enumerate(cols):
        dummies = pd.get_dummies(df[col])
        df_dummies = dummies.add_prefix(col + "_")
        df = df.join(df_dummies)
        df = df.drop([col], axis=1)
    tracker.analyze_changes(df)

    return df
