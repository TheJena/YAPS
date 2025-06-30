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
    tmp_df = tracker.subscribe(pd.DataFrame())
    tracker.analyze_changes(df, tmp_df)

    cols = ["X"]
    # Drop the 'X' column from the DataFrame
    df = df.drop(columns=cols)
    tracker.analyze_changes(df)

    cols = sorted(["Written", "Oral", "Combine"], key=str.lower)
    # Normalize the "Written", "Oral", "Combine" columns
    for col in cols:
        assert df[col].min() >= 0 and df[col].max() <= 100, str(col)
        df = df.assign(**{col: df[col].div(100).round(2)})
    tracker.analyze_changes(df)

    written_pass_thr = max(
        0.60,
        df["Written"].iloc[: floor(df.shape[0] / 2)].quantile(0.40),
    )
    selector = df["Written"] >= written_pass_thr
    # Filter w.r.t. 'Written' < written_pass_thr
    tmp_df = df.loc[~selector, :]
    tracker.analyze_changes(tmp_df)

    # Filter w.r.t. 'Written' >= written_pass_thr
    df = df.loc[selector, :]
    tracker.analyze_changes(df)

    cols = sorted(["Race", "Position", "Written"], key=str.lower)
    # Project the "Race", "Position", "Written" columns
    tmp_df = tmp_df.loc[:, cols]
    tracker.analyze_changes(tmp_df)

    col = "Combine"
    y_pass_thr = 0.60
    # Create y from 'Combine' >= 0.6
    df = df.assign(y=lambda _df: _df[col].ge(y_pass_thr).astype("boolean"))
    tracker.analyze_changes(df)

    # Vertical concatenation of Passed and Failed exams
    df = pd.concat([df, tmp_df], axis=0)
    tracker.analyze_changes(df)

    # Fill 'y' == Nan with False
    df.loc[df["y"].isna(), "y"] = False
    tracker.analyze_changes(df)

    cols = sorted(["Race", "Position"], key=str.lower)
    # One-hot encode the "Race", "Position" columns
    df = pd.get_dummies(df, columns=cols)
    tracker.analyze_changes(df)

    return df
