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

import numpy as np
import pandas as pd


def run_pipeline(args, tracker) -> None:
    input_path = args.dataset

    df = pd.read_csv(input_path)
    frac = args.frac

    if frac > 0.0 and frac < 1.0:
        df = df.sample(frac=frac)
    elif frac > 1.0:
        df = pd.concat([df] * int(frac), ignore_index=True)

    # Subscribe dataframe
    df = tracker.subscribe(df)

    df = df.replace(
        {
            "checking": {
                "A11": "check_low",
                "A12": "check_mid",
                "A13": "check_high",
                "A14": "check_none",
            },
            "credit_history": {
                "A30": "debt_none",
                "A31": "debt_noneBank",
                "A32": "debt_onSchedule",
                "A33": "debt_delay",
                "A34": "debt_critical",
            },
            "purpose": {
                "A40": "pur_newCar",
                "A41": "pur_usedCar",
                "A42": "pur_furniture",
                "A43": "pur_tv",
                "A44": "pur_appliance",
                "A45": "pur_repairs",
                "A46": "pur_education",
                "A47": "pur_vacation",
                "A48": "pur_retraining",
                "A49": "pur_business",
                "A410": "pur_other",
            },
            "savings": {
                "A61": "sav_small",
                "A62": "sav_medium",
                "A63": "sav_large",
                "A64": "sav_xlarge",
                "A65": "sav_none",
            },
            "employment": {
                "A71": "emp_unemployed",
                "A72": "emp_lessOne",
                "A73": "emp_lessFour",
                "A74": "emp_lessSeven",
                "A75": "emp_moreSeven",
            },
            "other_debtors": {
                "A101": "debtor_none",
                "A102": "debtor_coApp",
                "A103": "debtor_guarantor",
            },
            "property": {
                "A121": "prop_realEstate",
                "A122": "prop_agreement",
                "A123": "prop_car",
                "A124": "prop_none",
            },
            "other_inst": {
                "A141": "oi_bank",
                "A142": "oi_stores",
                "A143": "oi_none",
            },
            "housing": {
                "A151": "hous_rent",
                "A152": "hous_own",
                "A153": "hous_free",
            },
            "job": {
                "A171": "job_unskilledNR",
                "A172": "job_unskilledR",
                "A173": "job_skilled",
                "A174": "job_highSkill",
            },
            "phone": {"A191": 0, "A192": 1},
            "foreigner": {"A201": 1, "A202": 0},
            "label": {2: 0},
        }
    )

    status_mapping = {
        "A91": "divorced",
        "A92": "divorced",
        "A93": "single",
        "A95": "single",
    }

    df["status"] = df["personal_status"].map(status_mapping).fillna("married")

    # Translate gender values
    df["personal_status"] = np.where(
        df.personal_status == "A92",
        0,
        np.where(df.personal_status == "A95", 0, 1),
    )

    df = df.drop(["personal_status"], axis=1)

    columns = [
        "checking",
        "credit_history",
        "purpose",
        "savings",
        "employment",
        "other_debtors",
        "property",
        "other_inst",
        "housing",
        "job",
    ]

    for i, col in enumerate(columns):
        dummies = pd.get_dummies(df[col])
        df_dummies = dummies.add_prefix(col + "_")
        df = df.join(df_dummies)
        df = df.drop([col], axis=1)

    return df
