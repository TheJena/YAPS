from LLM.LLM_activities_descriptor import LLM_activities_descriptor
from LLM.LLM_formatter import LLM_formatter
from LLM.LLM_activities_used_columns import LLM_activities_used_columns
from graph.neo4j import Neo4jConnector, Neo4jFactory
from graph.structure import *

from graph.constants import *
from tracking.tracking import ProvenanceTracker
import argparse


def get_args() -> argparse.Namespace:
    """
    Parses command line arguments.
    """
    parser = argparse.ArgumentParser(description="Census Pipeline")
    parser.add_argument(
        "--dataset",
        type=str,
        default="datasets/generated_dataset.csv",
        help="Relative path to the dataset file",
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        default="pipelines/orders_pipeline.py",
        help="Relative path to the dataset file",
    )
    parser.add_argument(
        "--frac", type=float, default=1, help="Sampling fraction [0.0 - 1.0]"
    )

    return parser.parse_args()


# Standardize the structure of the file in a way that provenance is tracked
formatter = LLM_formatter(get_args().pipeline, api_key="MY_APY_KEY")
# Standardized file given by the LLM
extracted_file = formatter.standardize()
descriptor = LLM_activities_descriptor(extracted_file, api_key="MY_APY_KEY")
used_columns_giver = LLM_activities_used_columns(api_key="MY_APY_KEY")

from extracted_code import run_pipeline

# description of each activity. A list of dictionaries like { "act_name" : ("description of the operation", "code of the operation")}
activities_description = descriptor.descript()
activities_description_dict = (
    i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
        activities_description.replace("pipeline_operations = ", "")
    )
)
# print(activities_description_dict)

# Neo4j initialization
neo4j = Neo4jFactory.create_neo4j_queries(
    uri="bolt://localhost", user="MY_NEO4J_USERNAME", pwd="MY_NEO4J_PASSWORD"
)
neo4j.delete_all()
session = Neo4jConnector().create_session()
tracker = ProvenanceTracker(save_on_neo4j=True)
frac = 1

# running the preprocessing pipeline
run_pipeline(tracker, frac)

# Dictionary of all the df before and after the operations
changes = tracker.changes

# keeping current elements on the graph supporting the creation on neo4j
current_activities = []
current_entities = {}
current_columns = []
current_derivations = []
current_entities_column = []

current_relations_column = []

# Create the activities found by the llm
for act_name in activities_description_dict.keys():
    act_context, act_code = activities_description_dict[act_name]
    activity = create_activity(
        function_name=act_name, context=act_context, code=act_code
    )
    current_activities.append(activity)

# find the differnce of the df and create the entities
activities_and_entities = {}  # map of the entities modified by the activity
derivations = []
current_relations = []
for act in changes.keys():
    used_cols = None
    generated_entities = []
    used_entities = []
    invalidated_entities = []
    if act == 0:
        continue
    activity = current_activities[act - 1]
    df1 = changes[act]["before"]
    df2 = changes[act]["after"]
    activity_description, activity_code = activity["context"], activity["code"]
    # print(activity['function_name'])
    # print(df1)
    # print(df2)
    used_cols = i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
        used_columns_giver.give_columns(
            df1, df2, activity_code, activity_description
        )
    )
    # print(used_cols)
    # Initialize an empty list to store the differences
    diff_entities = []

    # Approach working when the number of rows is the same and the number of columns increase or is the same
    if len(df1.columns) <= len(df2.columns):
        # Iterate over the columns and rows to find differences
        used_col = True
        old_entity_in_col = []
        for col in df2.columns:
            if not used_col:
                used_entities.extend(old_entity_in_col)
            old_entity_in_col = []
            used_col = False
            for idx in df2.index:
                if idx in df1.index and col in df1.columns:
                    old_value = df1.at[idx, col]
                elif (
                    idx in df1.index
                    and df2.columns.get_loc(col) < len(df1.columns)
                    and (
                        list(df1.iloc[:, df2.columns.get_loc(col)])
                        == list(df2[col])
                    )
                ):
                    old_value = df1.iloc[
                        list(df1.index).index(idx), df2.columns.get_loc(col)
                    ]
                else:
                    old_value = "Not exist"
                new_value = df2.at[idx, col]
                # gen = False
                if (
                    old_value != new_value
                    or col != df1.columns[df2.columns.get_loc(col)]
                ):
                    # if (new_value, col, idx) in current_entities:
                    #     entity = current_entities[(new_value, col, idx)]
                    # else:
                    #     gen = True
                    entity = create_entity(new_value, col, idx)
                    if old_value != "Not exist":
                        old_entity = None
                        if (
                            old_value,
                            df1.columns[df2.columns.get_loc(col)],
                            idx,
                        ) in current_entities.keys():
                            old_entity = current_entities[
                                (
                                    old_value,
                                    df1.columns[df2.columns.get_loc(col)],
                                    idx,
                                )
                            ]
                        else:
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
                        if col in used_cols:
                            old_entity_in_col.append(old_entity)
                        derivations.append(
                            {
                                "gen": str(entity["id"]),
                                "used": str(old_entity["id"]),
                            }
                        )
                        used_entities.append(old_entity["id"])
                        used_col = True
                        invalidated_entities.append(old_entity["id"])
                    # if gen:
                    generated_entities.append(entity["id"])
                    current_entities[(new_value, col, idx)] = entity
        if not used_cols:
            used_entities.extend(old_entity_in_col)

    # if the number of columns decrease but the number of rows is still the same
    elif len(df1.columns) > len(df2.columns):
        # Iterate over the columns and rows to find differences
        unique_col_in_df1 = set(df1.columns) - set(df2.columns)
        unique_col_in_df2 = set(df2.columns) - set(df1.columns)
        # if the column is exclusively in the "before" dataframe
        used_col = True
        old_entity_in_col = []
        for col in unique_col_in_df1:
            if not used_col:
                used_entities.extend(old_entity_in_col)
            old_entity_in_col = []
            used_col = False
            for idx in df1.index:
                old_value = df1.at[idx, col]
                old_entity = None
                if (old_value, col, idx) in current_entities.keys():
                    old_entity = current_entities[(old_value, col, idx)]
                else:
                    old_entity = create_entity(old_value, col, idx)
                    current_entities[(old_value, col, idx)] = old_entity
                if col in used_cols:
                    old_entity_in_col.append(old_entity)
                invalidated_entities.append(old_entity["id"])
                used_entities.append(old_entity["id"])
                used_col = True
            if not used_cols:
                used_entities.extend(old_entity_in_col)
        # if the column is exclusively in the "after" dataframe
        for col in unique_col_in_df2:
            for idx in df2.index:
                new_value = df2.at[idx, col]
                gen = False
                # if (new_value, col, idx) in current_entities:
                #     new_entity = current_entities[(new_value, col, idx)]
                # else:
                #     gen = True
                new_entity = create_entity(new_value, col, idx)
                # if gen:
                current_entities[(new_value, col, idx)] = new_entity
                generated_entities.append(new_entity["id"])

        common_col = set(df1.columns).intersection(set(df2.columns))
        for col in common_col:
            for idx in df2.index:
                if idx in df1.index:
                    old_value = df1.at[idx, col]
                else:
                    old_value = "Not exist"
                new_value = df2.at[idx, col]
                if old_value != new_value:
                    # gen = False
                    if (new_value, col, idx) in current_entities:
                        continue
                    # else:
                    #     gen = True
                    entity = create_entity(new_value, col, idx)
                    if old_value != "Not exist":
                        old_entity = None
                        if (old_value, col, idx) in current_entities.keys():
                            old_entity = current_entities[
                                (old_value, col, idx)
                            ]
                        else:
                            old_entity = create_entity(old_value, col, idx)
                            current_entities[(old_value, col, idx)] = (
                                old_entity
                            )
                        derivations.append(
                            {
                                "gen": str(entity["id"]),
                                "used": str(old_entity["id"]),
                            }
                        )
                        used_entities.append(old_entity["id"])
                        invalidated_entities.append(old_entity["id"])
                    # if gen:
                    generated_entities.append(entity["id"])
                    current_entities[(new_value, col, idx)] = entity

    current_relations.append(
        create_relation(
            activity["id"],
            generated_entities,
            used_entities,
            invalidated_entities,
            same=False,
        )
    )

    # activities_and_entities[activity['id']] = diff_entities
    # print(activities_and_entities)

# Create constraints in Neo4j
neo4j.create_constraint(session=session)

# Add activities, entities, derivations, and relations to the Neo4j Graph
neo4j.add_activities(current_activities, session)
neo4j.add_entities(list(current_entities.values()))
neo4j.add_derivations(derivations)
neo4j.add_relations(current_relations)

pairs = []
for i in range(len(current_activities) - 1):
    pairs.append(
        {
            "act_in_id": current_activities[i]["id"],
            "act_out_id": current_activities[i + 1]["id"],
        }
    )

neo4j.add_next_operations(pairs)


# neo4j.add_columns(current_columns)
# neo4j.add_derivations(current_derivations)
# neo4j.add_relation_entities_to_column(current_entities_column)
# neo4j.add_derivations_columns(current_derivations)
# neo4j.add_relations(current_relations)
# neo4j.add_relations_columns(current_relations_column)

del current_activities[:]
del current_entities
del current_columns[:]
del current_derivations[:]
del current_entities_column[:]
del current_relations[:]
del current_relations_column[:]


session.close()
