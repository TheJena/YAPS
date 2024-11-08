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

    if args.frac > 0.0 and args.frac < 1.0:
        df = df.sample(frac=args.frac)
    elif args.frac > 1.0:
        df = pd.concat([df] * int(args.frac), ignore_index=True)

    # Subscribe dataframe
    df = tracker.subscribe(df)

    columns = ["car_price", "car_mileage"]
    for col in columns:
        df[col] = df[col].apply(
            lambda x: "{:.1f}k".format(x / 1000) if x >= 1000 else x
        )

    df = df.drop(
        [
            "car_transmission",
            "car_drive",
            "car_engine_capacity",
            "car_engine_hp",
        ],
        axis=1,
    )
    print(df)

    df.rename(columns={df.columns[0]: "car_id"}, inplace=True)
    print(df)

    cols = ["car_brand", "car_model", "car_city"]
    df[cols] = df[cols].applymap(str.strip)
    print(df)

    df["car_age_category"] = df["car_age"].apply(
        lambda age: "New" if age <= 3 else ("Middle" if age <= 9 else "Old")
    )
    print(df)

    return df
