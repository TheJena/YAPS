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


def run_pipeline(args, tracker) -> None:
    input_path = args.dataset

    df = pd.read_csv(input_path, index_col=0)

    # Subscribe dataframe
    df = tracker.subscribe(df)
    tmp_df = tracker.subscribe(pd.DataFrame())

    df = df.drop(columns="X")

    for col in ("Written", "Oral", "Combine"):
        assert df[col].min() >= 0 and df[col].max() <= 100, str(col)
        df = df.assign(**{col: df[col].div(100).round(2)})

    written_pass_thr = max(
        0.60,
        df["Written"].iloc[: floor(df.shape[0] / 2)].quantile(0.40),
    )
    selector = df["Written"] >= written_pass_thr
    tmp_df = df.loc[~selector, :]
    df = df.loc[selector, :]

    tmp_df = tmp_df.loc[:, ["Race", "Position", "Written"]]

    df = df.assign(y=lambda _df: _df["Combine"].ge(0.60).astype("boolean"))

    df = pd.concat([df, tmp_df], axis=0)

    df.loc[df["y"].isna(), "y"] = False

    df = pd.get_dummies(df, columns=["Race", "Position"])

    return df
