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

import pandas as pd


def run_pipeline(args, tracker) -> None:
    input_path = args.dataset

    df = pd.read_csv(input_path, index_col=0)

    # Subscribe dataframe
    df = tracker.subscribe(df)

    df = df.drop(columns="X")

    # During Exploratory Data Analysis, the top 40% of the first 20
    # candidates had a written score >= of 74; this determined the
    # admission score for the oral exam
    df = df[df["Written"] >= 74]

    df = pd.get_dummies(
        df,
        columns=["Race"],
    )

    return df
