#!/usr/bin/env python3
# coding: utf-8
#
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Copyright (C) 2025 Federico Motta <federico.motta@unimore.it>
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

from math import floor
import pandas as pd
import sys

sys.path.append("../../")


def run_pipeline(args, tracker) -> None:
    input_path = args.dataset

    # Read the input CSV file into a pandas DataFrame
    df = pd.read_csv(input_path, index_col=0)

    # Subscribe dataframe to tracker for provenance analysis
    df = tracker.subscribe(df)
    tracker.analyze_changes(df)

    cols = ["X"]
    # Drop the 'X' column from the DataFrame
    df = df.drop(columns=cols)
    tracker.analyze_changes(df)

    selector = df["Written"] >= 74
    # Filter the DataFrame to only include rows where 'Written' is >= 74
    df = df[selector]
    tracker.analyze_changes(df)

    cols = ["Race"]
    # One-hot encode the 'Race' column
    df = pd.get_dummies(df, columns=cols)
    tracker.analyze_changes(df)

    return df
