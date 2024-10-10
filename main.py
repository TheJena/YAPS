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

from LLM.LLM_activities_descriptor import LLM_activities_descriptor
from LLM.LLM_activities_used_columns import LLM_activities_used_columns
from LLM.LLM_formatter import LLM_formatter
from SECRET import MY_API_KEY, MY_NEO4J_PASSWORD, MY_NEO4J_USERNAME
from column_approach import column_vision
from column_entity_approach import column_entitiy_vision
from extracted_code import run_pipeline
from graph.neo4j import Neo4jConnector, Neo4jFactory
from graph.structure import create_activity
from tracking.tracking import ProvenanceTracker
from utils import (
    i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine,
)
import argparse


def wrapper_run_pipeline(args, tracker):
    try:
        # Esegui la funzione run_pipeline; potresti modificare
        # run_pipeline per restituire lo stato parziale se possibile
        run_pipeline(args, tracker)  # La funzione non viene modificata
    except Exception as e:
        exception_type = type(e).__name__
        exception_message = str(e)
        print(f"Eccezione catturata: {exception_type} - {exception_message}")
        return f"{exception_type} - {exception_message}"
    return " "


def get_args() -> argparse.Namespace:
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser(description="TODO")
    parser.add_argument(
        "--dataset",
        type=str,
        default="datasets/car.csv",
        help="Relative path to the dataset file",
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default="pipelines/raw/car.py",
        help="Relative path to the dataset file",
    )
    parser.add_argument(
        "--frac", type=float, default=0.1, help="Sampling fraction [0.0 - 1.0]"
    )
    parser.add_argument(
        "--granularity_level",
        type=int,
        default=3,
        help="Granularity level: 1, 2 or 3",
    )
    parser.add_argument(
        "--entity_type_level",
        type=int,
        default=2,
        help="Entity level: 1 for entities and columns and 2 for columns",
    )

    return parser.parse_args()


# Standardize the structure of the file in a way that provenance is tracked
formatter = LLM_formatter(get_args().pipeline)
# Standardized file given by the LLM
extracted_file = formatter.standardize()

descriptor = LLM_activities_descriptor(extracted_file, api_key=MY_API_KEY)
used_columns_giver = LLM_activities_used_columns(api_key=MY_API_KEY)

# description of each activity. A list of dictionaries like {
# "act_name" : ("description of the operation", "code of the
# operation")}
activities_description = descriptor.descript()
# print(activities_description)
activities_description_dict = (
    i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
        activities_description.replace("pipeline_operations = ", "")
    )
)
# print(activities_description_dict)

# Neo4j initialization
neo4j = Neo4jFactory.create_neo4j_queries(
    uri="bolt://localhost", user=MY_NEO4J_USERNAME, pwd=MY_NEO4J_PASSWORD
)
neo4j.delete_all()
session = Neo4jConnector().create_session()
tracker = ProvenanceTracker(save_on_neo4j=True)

# running the preprocessing pipeline
exception = wrapper_run_pipeline(get_args(), tracker)

# Dictionary of all the df before and after the operations
changes = tracker.changes
# print(changes)

current_activities = []
current_entities = {}
current_columns = {}
current_derivations = []
current_entities_column = []
entities_to_keep = []
derivations = []
derivations_column = []
current_relations = []
current_relations_column = []
current_columns_to_entities = {}

loop = True
activity_to_zoom = None
while loop:
    # Create the activities found by the llm
    for act_name in activities_description_dict.keys():
        act_context, act_code = activities_description_dict[act_name]
        activity = create_activity(
            function_name=act_name,
            context=act_context,
            code=act_code,
            exception_text=exception,
        )
        current_activities.append(activity)

    if get_args().entity_type_level == 1:
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
            changes, current_activities, get_args(), activity_to_zoom
        )
    else:
        (
            current_relations_column,
            current_columns,
            derivations_column,
        ) = column_vision(changes, current_activities)

    # Create constraints in Neo4j
    neo4j.create_constraint(session=session)

    # Add activities, entities, derivations, and relations to the Neo4j Graph
    neo4j.add_activities(current_activities, session)
    if get_args().granularity_level == 1:
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

    relations = []
    for act in current_columns_to_entities.keys():
        relation = []
        relation.append(act)
        relation.append(current_columns_to_entities[act])
        relations.append(relation)
    neo4j.add_relation_entities_to_column(relations)

    pairs = []
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
