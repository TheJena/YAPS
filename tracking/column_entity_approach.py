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

from LLM.LLM_activities_used_columns import LLM_activities_used_columns
from graph.structure import (
    create_entity,
    create_column,
    create_relation,
    create_relation_column,
)
from logging import debug
from utils import (
    i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine,
    keep_random_element_in_place,
)
import pandas as pd


def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def column_entitiy_vision(changes, current_activities, args):
    assert args.prov_entity_level

    # keeping current elements on the graph supporting the creation on neo4j
    current_entities = dict()
    current_columns = dict()

    used_columns_giver = LLM_activities_used_columns()
    # keeping current elements on the graph supporting the creation on neo4j
    entities_to_keep = list()

    # find the differnce of the df and create the entities
    derivations = list()
    derivations_column = list()
    current_relations = list()
    current_relations_column = list()
    current_columns_to_entities = dict()
    for act in changes.keys():
        used_cols = None
        generated_entities = list()
        used_entities = list()
        invalidated_entities = list()
        generated_columns = list()
        used_columns = list()
        invalidated_columns = list()
        if act == 0:
            continue
        activity = current_activities[act - 1]
        df1 = changes[act]["before"]
        df2 = changes[act]["after"]
        activity["runtime_exceptions"] = "No exceptions occurred"

        activity_description, activity_code = (
            activity["context"],
            activity["code"],
        )
        debug(f"{activity['function_name']=}")
        used_columns_string = used_columns_giver.give_columns(
            df1, df2, activity_code, activity_description
        )
        debug(f"{used_columns_string=}")
        used_cols = i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(  # noqa
            used_columns_string
        )
        debug(f"{used_cols=}")

        # Approach working when the number of rows is the same and
        # the number of columns increase or is the same
        # if len(df1.columns) <= len(df2.columns):

        # Iterate over the columns and rows to find differences
        unique_col_in_df1 = set(df1.columns) - set(df2.columns)
        unique_col_in_df2 = set(df2.columns) - set(df1.columns)
        # if the column is exclusively in the "before" dataframe
        unique_df1_col = list()
        for col in unique_col_in_df1:
            # control il the column already exist or create it
            val_col = str(df1[col].tolist())
            idx_col = str(df1.index.tolist())
            new_column = None
            if (val_col, idx_col, col) not in current_columns.keys():
                new_column = create_column(val_col, idx_col, col)
                current_columns[(val_col, idx_col, col)] = new_column
                current_columns_to_entities[new_column["id"]] = list()
            else:
                new_column = current_columns[(val_col, idx_col, col)]
            unique_df1_col.append(new_column)
            used_columns.append(new_column["id"])
            invalidated_columns.append(new_column["id"])
            for idx in df1.index:
                old_value = df1.at[idx, col]
                old_entity = None
                if (old_value, col, idx) in current_entities.keys():
                    old_entity = current_entities[(old_value, col, idx)]
                else:
                    old_entity = create_entity(old_value, col, idx)
                    current_entities[(old_value, col, idx)] = old_entity
                invalidated_entities.append(old_entity["id"])
                used_entities.append(old_entity["id"])
                current_columns_to_entities[new_column["id"]].append(  #
                    old_entity["id"]
                )
        # if the column is exclusively in the "after" dataframe
        for col in unique_col_in_df2:
            # control il the column already exist or create it
            val_col = str(df2[col].tolist())
            idx_col = str(df2.index.tolist())
            old_col = None
            if (val_col, idx_col, col) not in current_columns.keys():
                new_column = create_column(val_col, idx_col, col)
                generated_columns.append(new_column["id"])
                current_columns[(val_col, idx_col, col)] = new_column
                current_columns_to_entities[new_column["id"]] = list()
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
                        old_col = column["name"]
                        break
            for idx in df2.index:
                new_value = df2.at[idx, col]
                new_entity = create_entity(new_value, col, idx)
                if old_col and df1.at[idx, old_col]:
                    old_value = df1.at[idx, old_col]
                    old_entity = current_entities[(old_value, old_col, idx)]
                    derivations.append(
                        {
                            "gen": str(new_entity["id"]),
                            "used": str(old_entity["id"]),
                        }
                    )
                current_entities[(new_value, col, idx)] = new_entity
                generated_entities.append(new_entity["id"])
                current_columns_to_entities[new_column["id"]].append(  #
                    new_entity["id"]
                )

        common_col = set(df1.columns).intersection(set(df2.columns))
        for col in common_col:
            new_column = None

            # verify if a column is used and in that case add it to
            # used columns
            used_column = None
            if col in used_cols:
                val_col = str(df1[col].tolist())
                idx_col = str(df1.index.tolist())
                if (val_col, idx_col, col) not in current_columns.keys():
                    used_column = create_column(val_col, idx_col, col)
                    current_columns[(val_col, idx_col, col)] = used_column
                    current_columns_to_entities[used_column["id"]] = list()
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
                        if pd.isna(new_val) and pd.isna(old_val):
                            continue
                    if (new_value, col, idx) in current_entities:
                        continue
                    # control il the column already exist or create it
                    val_col = str(df2[col].tolist())
                    idx_col = str(df2.index.tolist())
                    if (val_col, idx_col, col) not in current_columns.keys():
                        new_column = create_column(val_col, idx_col, col)
                        generated_columns.append(new_column["id"])
                        current_columns[(val_col, idx_col, col)] = new_column
                        current_columns_to_entities[new_column["id"]] = list()
                    else:
                        new_column = current_columns[(val_col, idx_col, col)]

                    entity = create_entity(new_value, col, idx)
                    if old_value != "Not exist":
                        # same control but for the before df, to get
                        # the used columns
                        old_column = None
                        val_old_col = str(df1[col].tolist())
                        idx_old_col = str(df1.index.tolist())
                        if (
                            val_old_col,
                            idx_old_col,
                            col,
                        ) not in current_columns.keys():
                            old_column = create_column(
                                val_old_col,
                                idx_old_col,
                                col,
                            )
                            current_columns[
                                (val_old_col, idx_old_col, col)
                            ] = old_column
                            current_columns_to_entities[old_column["id"]] = (
                                list()
                            )
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
                        old_entity = None
                        if (old_value, col, idx) in current_entities.keys():
                            old_entity = current_entities[
                                (
                                    old_value,
                                    col,
                                    idx,
                                )
                            ]
                        else:
                            old_entity = create_entity(old_value, col, idx)
                            current_entities[
                                (
                                    old_value,
                                    col,
                                    idx,
                                )
                            ] = old_entity
                        derivations.append(
                            {
                                "gen": str(entity["id"]),
                                "used": str(old_entity["id"]),
                            }
                        )
                        used_entities.append(old_entity["id"])
                        invalidated_entities.append(old_entity["id"])
                        current_columns_to_entities[old_column["id"]].append(
                            old_entity["id"]
                        )
                    generated_entities.append(entity["id"])
                    current_entities[(new_value, col, idx)] = entity
                    current_columns_to_entities[new_column["id"]].append(  #
                        entity["id"]
                    )
        # # Iterate over the columns and rows to find differences
        unique_rows_in_df1 = set(df1.index) - set(df2.index)
        if len(unique_rows_in_df1) > 0:
            for col in df2.columns:
                # control il the column already exist or create it
                val_col = str(df2[col].tolist())
                idx_col = str(df2.index.tolist())
                new_column = None
                if (val_col, idx_col, col) not in current_columns.keys():
                    new_column = create_column(val_col, idx_col, col)
                    current_columns[(val_col, idx_col, col)] = new_column
                    current_columns_to_entities[new_column["id"]] = list()
                    generated_columns.append(new_column["id"])
                else:
                    new_column = current_columns[(val_col, idx_col, col)]

                for idx in unique_rows_in_df1:
                    if idx in df1.index and col in df1.columns:
                        val_col = str(df1[col].tolist())
                        idx_col = str(df1.index.tolist())
                        old_column = None
                        if (
                            val_col,
                            idx_col,
                            col,
                        ) not in current_columns.keys():
                            old_column = create_column(val_col, idx_col, col)
                            current_columns[
                                (
                                    val_col,
                                    idx_col,
                                    col,
                                )
                            ] = old_column
                            current_columns_to_entities[old_column["id"]] = (
                                list()
                            )
                        else:
                            old_column = current_columns[
                                (
                                    val_col,
                                    idx_col,
                                    col,
                                )
                            ]
                        old_value = df1.at[idx, col]
                        old_entity = create_entity(
                            old_value,
                            df1.columns[df2.columns.get_loc(col)],
                            idx,
                        )
                        current_entities[
                            (
                                old_value,
                                df1.columns[df2.columns.get_loc(col)],
                                idx,
                            )
                        ] = old_entity
                        current_columns[(val_col, idx_col, col)] = old_column
                        used_columns.append(old_column["id"])
                        invalidated_columns.append(old_column["id"])
                        if new_column and new_column["id"] != old_column["id"]:
                            derivations_column.append(
                                {
                                    "gen": str(new_column["id"]),
                                    "used": str(old_column["id"]),
                                }
                            )
                        used_entities.append(old_entity["id"])
                        invalidated_entities.append(old_entity["id"])
                        current_columns_to_entities[old_column["id"]].append(
                            old_entity["id"]
                        )

        if args.granularity_level in (1, 2):
            gen_element = keep_random_element_in_place(generated_entities)
            inv_elem = None
            if gen_element:
                entities_to_keep.append(gen_element)
            used_elem = keep_random_element_in_place(used_entities)
            if used_elem:
                if used_elem in invalidated_entities:
                    invalidated_entities.clear()
                    invalidated_entities.append(used_elem)
                entities_to_keep.append(used_elem)
            else:
                inv_elem = keep_random_element_in_place(invalidated_entities)
            if inv_elem:
                entities_to_keep.append(inv_elem)

        current_relations_column.append(
            create_relation_column(
                activity["id"],
                generated_columns,
                used_columns,
                invalidated_columns,
                same=False,
            )
        )
        current_relations.append(
            create_relation(
                activity["id"],
                generated_entities,
                used_entities,
                invalidated_entities,
                same=False,
            )
        )

    # unified interface with column_approach.column_vision()
    return (
        current_entities,
        current_columns,
        current_relations,
        current_relations_column,
        derivations,
        derivations_column,
        current_columns_to_entities,
        entities_to_keep,
    )
