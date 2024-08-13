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


import sys
import argparse
import pandas as pd
from graph.logger import CustomLogger
import numpy as np


def stratified_sample(df, frac):
    """
    Perform stratified sampling on a DataFrame.
    """
    if frac > 0.0 and frac < 1.0:
        # Infer categorical columns for stratification
        stratify_columns = df.select_dtypes(
            include=["object"]
        ).columns.tolist()

        # Check if any class in stratify columns has fewer than 2 members
        for col in stratify_columns:
            value_counts = df[col].value_counts()
            if value_counts.min() >= 2:
                # Perform stratified sampling for this column
                stratified_df = df.groupby(col, group_keys=False).apply(
                    lambda x: x.sample(frac=frac)
                )
                return stratified_df.reset_index(drop=True)

        # If no suitable stratification column is found, fall back to random sampling
        sampled_df = df.sample(frac=frac).reset_index(drop=True)
    else:
        sampled_df = df
    return sampled_df


def run_pipeline(args, tracker) -> None:

    input_path = args.dataset

    df = pd.read_csv(input_path, header=0)

    if args.frac != 0.0:
        df = df.sample(frac=args.frac)

    # Subscribe dataframe
    df = tracker.subscribe(df)
    tracker.analyze_changes(df)

    # Drop columns not in the specified list
    columns = [
        "age",
        "c_charge_degree",
        "race",
        "sex",
        "priors_count",
        "days_b_screening_arrest",
        "two_year_recid",
        "c_jail_in",
        "c_jail_out",
    ]
    df = df.drop(df.columns.difference(columns), axis=1)
    tracker.analyze_changes(df)

    # Drop rows with missing values
    df = df.dropna()
    tracker.analyze_changes(df)

    # Convert race column to binary values
    df["race"] = [0 if r != "Caucasian" else 1 for r in df["race"]]
    tracker.analyze_changes(df)

    # Rename two_year_recid column to label
    df = df.rename({"two_year_recid": "label"}, axis=1)
    tracker.analyze_changes(df)

    # Reverse label for consistency with function defs: 1 means no recid (good), 0 means recid (bad)
    df["label"] = [0 if l == 1 else 1 for l in df["label"]]
    tracker.analyze_changes(df)

    # Calculate jailtime in days
    df["jailtime"] = (
        pd.to_datetime(df.c_jail_out) - pd.to_datetime(df.c_jail_in)
    ).dt.days
    tracker.analyze_changes(df)

    # Drop c_jail_in and c_jail_out columns
    df = df.drop(["c_jail_in", "c_jail_out"], axis=1)
    tracker.analyze_changes(df)

    # Convert c_charge_degree column to binary values
    df["c_charge_degree"] = [
        0 if s == "M" else 1 for s in df["c_charge_degree"]
    ]
    tracker.analyze_changes(df)
