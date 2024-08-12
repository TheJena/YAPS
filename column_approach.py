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
# This file is part of a provenance capturing suite (originally named DPDS)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from graph.structure import *
from graph.constants import *
from LLM.LLM_activities_used_columns import LLM_activities_used_columns
import math
from SECRET import MY_API_KEY


def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def column_vision(changes, current_activities):
    derivations_column = []
    current_relations_column = []
    current_columns = {}

    used_columns_giver = LLM_activities_used_columns(api_key=MY_API_KEY)

    for act in changes.keys():
        generated_columns = []
        used_columns = []
        invalidated_columns = []
        if act == 0:
            continue
        activity = current_activities[act - 1]
        df1 = changes[act]["before"]
        df2 = changes[act]["after"]
        activity_description, activity_code = (
            activity["context"],
            activity["code"],
        )
        prova = used_columns_giver.give_columns(
            df1, df2, activity_code, activity_description
        )
        print(prova)
        used_cols = i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
            prova
        )
        print(used_cols)
        # print(activity['function_name'], df1, df2)
        # Iterate over the columns and rows to find differences
        unique_col_in_df1 = set(df1.columns) - set(df2.columns)
        unique_col_in_df2 = set(df2.columns) - set(df1.columns)
        # if the column is exclusively in the "before" dataframe
        unique_df1_col = []
        for col in unique_col_in_df1:
            # if the column already exist or create it
            val_col = str(df1[col].tolist())
            idx_col = str(df1.index.tolist())
            new_column = None
            if (val_col, idx_col, col) not in current_columns.keys():
                new_column = create_column(val_col, idx_col, col)
                current_columns[(val_col, idx_col, col)] = new_column
            else:
                new_column = current_columns[(val_col, idx_col, col)]
            unique_df1_col.append(new_column)
            used_columns.append(new_column["id"])
            invalidated_columns.append(new_column["id"])
        # if the column is exclusively in the "after" dataframe
        for col in unique_col_in_df2:
            # see if the column already exist or create it
            val_col = str(df2[col].tolist())
            idx_col = str(df2.index.tolist())
            if (val_col, idx_col, col) not in current_columns.keys():
                new_column = create_column(val_col, idx_col, col)
                generated_columns.append(new_column["id"])
                current_columns[(val_col, idx_col, col)] = new_column
                for column in unique_df1_col:
                    if (
                        new_column["index"] == column["index"]
                        and new_column["value"] == column["value"]
                    ):
                        derivations_column.append(
                            {
                                "gen": str(new_column["id"]),
                                "used": str(column["id"]),
                            }
                        )
                        break
        common_col = set(df1.columns).intersection(set(df2.columns))
        for col in common_col:
            new_column = None
            used_column = None
            if col in used_cols:
                val_col = str(df1[col].tolist())
                idx_col = str(df1.index.tolist())
                if (val_col, idx_col, col) not in current_columns.keys():
                    used_column = create_column(val_col, idx_col, col)
                    current_columns[(val_col, idx_col, col)] = used_column
                else:
                    used_column = current_columns[(val_col, idx_col, col)]
                if used_column:
                    used_columns.append(used_column["id"])
            for idx in df2.index:
                if idx in df1.index:
                    old_value = df1.at[idx, col]
                else:
                    old_value = "Not exist"
                new_value = df2.at[idx, col]
                if old_value != new_value:
                    if is_number(old_value) and is_number(new_value):
                        old_val = float(old_value)
                        new_val = float(new_value)
                        if math.isnan(new_val) and math.isnan(old_val):
                            continue
                    # if the column already exist or create it
                    val_col = str(df2[col].tolist())
                    idx_col = str(df2.index.tolist())
                    if (val_col, idx_col, col) not in current_columns.keys():
                        new_column = create_column(val_col, idx_col, col)
                        generated_columns.append(new_column["id"])
                        current_columns[(val_col, idx_col, col)] = new_column
                    else:
                        new_column = current_columns[(val_col, idx_col, col)]

                    if old_value != "Not exist":
                        # same but for the before df, to get the used columns
                        old_column = None
                        val_old_col = str(df1[col].tolist())
                        idx_old_col = str(df1.index.tolist())
                        if (
                            val_old_col,
                            idx_old_col,
                            col,
                        ) not in current_columns.keys():
                            old_column = create_column(
                                val_old_col, idx_old_col, col
                            )
                            current_columns[
                                (val_old_col, idx_old_col, col)
                            ] = old_column
                        else:
                            old_column = current_columns[
                                (val_old_col, idx_old_col, col)
                            ]
                        if new_column and new_column["id"] != old_column["id"]:
                            derivations_column.append(
                                {
                                    "gen": str(new_column["id"]),
                                    "used": str(old_column["id"]),
                                }
                            )
                        used_columns.append(old_column["id"])
                        invalidated_columns.append(old_column["id"])
                        break
            unique_rows_in_df1 = set(df1.index) - set(df2.index)
            for idx in unique_rows_in_df1:
                if idx in df1.index and col in df1.columns:
                    # the old column that with the unique row
                    val_col = str(df1[col].tolist())
                    idx_col = str(df1.index.tolist())
                    if (val_col, idx_col, col) not in current_columns.keys():
                        old_column = create_column(val_col, idx_col, col)
                        current_columns[(val_col, idx_col, col)] = old_column
                    else:
                        old_column = current_columns[(val_col, idx_col, col)]
                    current_columns[(val_col, idx_col, col)] = old_column
                    used_columns.append(old_column["id"])
                    invalidated_columns.append(old_column["id"])
                    # the new column without the unique row
                    val_new_col = str(df2[col].tolist())
                    idx_new_col = str(df2.index.tolist())
                    if (
                        val_new_col,
                        idx_new_col,
                        col,
                    ) not in current_columns.keys():
                        new_column = create_column(
                            val_new_col, idx_new_col, col
                        )
                        current_columns[(val_new_col, idx_new_col, col)] = (
                            new_column
                        )
                    else:
                        new_column = current_columns[
                            (val_new_col, idx_new_col, col)
                        ]
                    current_columns[(val_new_col, idx_new_col, col)] = (
                        new_column
                    )
                    generated_columns.append(new_column["id"])
                    if new_column and new_column["id"] != old_column["id"]:
                        derivations_column.append(
                            {
                                "gen": str(new_column["id"]),
                                "used": str(old_column["id"]),
                            }
                        )
                    break

        current_relations_column.append(
            create_relation_column(
                activity["id"],
                generated_columns,
                used_columns,
                invalidated_columns,
                same=False,
            )
        )
    return current_relations_column, current_columns, derivations_column
