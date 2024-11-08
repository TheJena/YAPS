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


from column_approach import column_vision
from column_entity_approach import column_entitiy_vision
from graph.neo4j import Neo4jConnector, Neo4jFactory
from graph.structure import create_activity
from LLM.LLM_activities_descriptor import LLM_activities_descriptor
from LLM.LLM_activities_used_columns import LLM_activities_used_columns
from LLM.LLM_formatter import LLM_formatter
from os.path import abspath, lexists
from os import remove, symlink
from SECRET import MY_NEO4J_PASSWORD, MY_NEO4J_USERNAME
from tracking.tracking import ProvenanceTracker
from utils import parsed_args, serialize, yaml_load
import extracted_code
import importlib


def wrapper_run_pipeline(args, tracker):
    pipeline_symlink = "extracted_code.py"
    assert abspath(pipeline_symlink) == abspath(extracted_code.__file__), str(
        f"Please update LLM_formatter.standardize() default output, "
        f"{pipeline_symlink=}; expected {abspath(extracted_code.__file__)!r}"
    )

    if lexists(pipeline_symlink):
        remove(pipeline_symlink)
    assert not lexists(pipeline_symlink), "check the above remove() on windows"

    pipeline_path = abspath(parsed_args().formatted_pipeline.name)
    symlink(pipeline_path, abspath(f"./{pipeline_symlink}"))
    importlib.reload(extracted_code)

    try:
        ret = extracted_code.run_pipeline(args, tracker)
    except Exception as e:
        exception_type = type(e).__name__
        exception_message = str(e)
        print(f"Eccezione catturata: {exception_type} - {exception_message}")
        return f"{exception_type} - {exception_message}"
    else:
        return " "
    finally:
        if parsed_args().output is not None:
            serialize(ret, parsed_args().output)


cli_args = parsed_args()

if cli_args.formatted_pipeline is None:
    # Standardize the structure of the file in a way that provenance
    # is tracked
    formatter = LLM_formatter(cli_args.raw_pipeline)
    # Standardized file given by the LLM
    extracted_file = formatter.standardize(cli_args.formatted_pipeline)
else:
    extracted_file = cli_args.formatted_pipeline

if cli_args.pipeline_description is None:
    descriptor = LLM_activities_descriptor(extracted_file)

    # description of each activity. A list of dictionaries like {
    # "act_name" : ("description of the operation", "code of the
    # operation")}
    activities_descr_dict = descriptor.descript(cli_args.pipeline_description)
else:
    activities_descr_dict = yaml_load(cli_args.pipeline_description)


used_columns_giver = LLM_activities_used_columns()
# print(
#     used_columns_giver.give_columns(
#         df_before, df_after, code, description
#     )
# )


# Neo4j initialization
neo4j = Neo4jFactory.create_neo4j_queries(
    uri="bolt://localhost", user=MY_NEO4J_USERNAME, pwd=MY_NEO4J_PASSWORD
)
neo4j.delete_all()
session = Neo4jConnector().create_session()
tracker = ProvenanceTracker(save_on_neo4j=True)

# running the preprocessing pipeline
exception = wrapper_run_pipeline(cli_args, tracker)

# Dictionary of all the df before and after the operations
changes = tracker.changes
# print(changes)

current_activities = list()
current_entities = dict()
current_columns = dict()
current_derivations = list()
current_entities_column = list()
entities_to_keep = list()
derivations = list()
derivations_column = list()
current_relations = list()
current_relations_column = list()
current_columns_to_entities = dict()

loop = True
activity_to_zoom = None
while loop:
    # Create the activities found by the llm
    for act_name in activities_descr_dict.keys():
        act_context, act_code = activities_descr_dict[act_name]
        activity = create_activity(
            function_name=act_name,
            context=act_context,
            code=act_code,
            exception_text=exception,
        )
        current_activities.append(activity)

    if cli_args.prov_entity_level:
        (
            current_entities,
            current_columns,
            current_relations,
            current_relations_column,
            derivations,
            derivations_column,
            current_columns_to_entities,
            entities_to_keep,
        ) = column_entitiy_vision(
            changes, current_activities, cli_args, activity_to_zoom
        )
    elif cli_args.prov_column_level:
        (
            current_relations_column,
            current_columns,
            derivations_column,
        ) = column_vision(changes, current_activities)

    # Create constraints in Neo4j
    neo4j.create_constraint(session=session)

    # Add activities, entities, derivations, and relations to the Neo4j Graph
    neo4j.add_activities(current_activities, session)
    if cli_args.granularity_level == 1:
        filtered_list = [
            entity
            for entity in current_entities.values()
            if entity["id"] in entities_to_keep
        ]
        neo4j.add_entities(filtered_list)
    else:
        neo4j.add_entities(list(current_entities.values()))
    neo4j.add_columns(list(current_columns.values()))
    neo4j.add_derivations(derivations)
    neo4j.add_relations(current_relations)
    neo4j.add_relations_columns(current_relations_column)
    neo4j.add_derivations_columns(derivations_column)

    relations = list()
    for act in current_columns_to_entities.keys():
        relation = list()
        relation.append(act)
        relation.append(current_columns_to_entities[act])
        relations.append(relation)
    neo4j.add_relation_entities_to_column(relations)

    pairs = list()
    for i in range(len(current_activities) - 1):
        pairs.append(
            {
                "act_in_id": current_activities[i]["id"],
                "act_out_id": current_activities[i + 1]["id"],
            }
        )

    neo4j.add_next_operations(pairs)

    del current_activities[:]
    del current_entities
    del current_columns
    del derivations[:]
    del derivations_column[:]
    del current_relations[:]
    del current_relations_column[:]
    del current_columns_to_entities
    # print("if you want to zoom on one activity select the succession
    # number of the desired activity, otherwise type 'N' ") answer =
    # input(">") if answer == 'N':
    loop = False
    # neo4j.delete_all()
    # else:
    #     neo4j.delete_all()
    #     activity_to_zoom = int(answer)

session.close()
