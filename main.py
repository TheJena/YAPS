from LLM.LLM_activities_descriptor import LLM_activities_descriptor
from LLM.LLM_formatter import LLM_formatter
from LLM.LLM_activities_used_columns import LLM_activities_used_columns
from graph.neo4j import Neo4jConnector, Neo4jFactory
from graph.structure import *
from utils import *

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

    parser.add_argument(
        "--granularity_level",
        type=int,
        default=3,
        help="Granularity level: 1, 2 or 3",
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
current_columns = {}
current_derivations = []
current_entities_column = []

entities_to_keep = []

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
derivations_column = []
current_relations = []
current_relations_column = []
current_columns_to_entities = {}
for act in changes.keys():
    used_cols = None
    generated_entities = []
    used_entities = []
    invalidated_entities = []
    generated_columns = []
    used_columns = []
    invalidated_columns = []
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
        unique_rows_in_df1 = set(df1.index) - set(df2.index)
        for col in df2.columns:
            if not used_col:
                used_entities.extend(old_entity_in_col)
            old_entity_in_col = []
            used_col = False
            new_column = None
            for idx in df2.index:
                old_col_name = col
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
                    old_col_name = df1.columns[df2.columns.get_loc(col)]
                else:
                    old_value = "Not exist"
                new_value = df2.at[idx, col]
                # gen = False
                if (
                    old_value != new_value
                    or col != df1.columns[df2.columns.get_loc(col)]
                ):
                    # control the column already exist or create it
                    val_col = str(df2[col].tolist())
                    idx_col = str(df2.index.tolist())
                    if (val_col, idx_col, col) not in current_columns.keys():
                        new_column = create_column(val_col, idx_col, col)
                        generated_columns.append(new_column["id"])
                        current_columns[(val_col, idx_col, col)] = new_column
                        current_columns_to_entities[new_column["id"]] = []
                    entity = create_entity(new_value, col, idx)
                    if old_value != "Not exist":
                        # same control but for the before df, to get the used columns
                        old_column = None
                        val_old_col = str(df1[old_col_name].tolist())
                        idx_old_col = str(df1.index.tolist())
                        if (
                            val_old_col,
                            idx_old_col,
                            old_col_name,
                        ) not in current_columns.keys():
                            old_column = create_column(
                                val_old_col, idx_old_col, old_col_name
                            )
                            current_columns[
                                (val_old_col, idx_old_col, old_col_name)
                            ] = old_column
                            current_columns_to_entities[old_column["id"]] = []
                        else:
                            old_column = current_columns[
                                (val_old_col, idx_old_col, old_col_name)
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
                        current_columns_to_entities[old_column["id"]].append(
                            old_entity["id"]
                        )
                    # if gen:
                    generated_entities.append(entity["id"])
                    current_entities[(new_value, col, idx)] = entity
                    current_columns_to_entities[new_column["id"]].append(
                        entity["id"]
                    )

            for idx in unique_rows_in_df1:
                if idx in df1.index and col in df1.columns:
                    val_col = str(df1[col].tolist())
                    idx_col = str(df1.index.tolist())
                    if (val_col, idx_col, col) not in current_columns.keys():
                        old_column = create_column(val_col, idx_col, col)
                        current_columns[(val_col, idx_col, col)] = old_column
                        current_columns_to_entities[old_column["id"]] = []
                    else:
                        old_column = current_columns[(val_col, idx_col, col)]
                    old_value = df1.at[idx, col]
                    old_entity = create_entity(
                        old_value, df1.columns[df2.columns.get_loc(col)], idx
                    )
                    current_entities[
                        (old_value, df1.columns[df2.columns.get_loc(col)], idx)
                    ] = old_entity
                    current_columns[(val_col, idx_col, col)] = old_column
                    used_entities.append(old_entity["id"])
                    used_col = True
                    invalidated_entities.append(old_entity["id"])
                    current_columns_to_entities[old_column["id"]].append(
                        old_entity["id"]
                    )

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
            # control il the column already exist or create it
            val_col = str(df1[col].tolist())
            idx_col = str(df1.index.tolist())
            new_column = None
            if (val_col, idx_col, col) not in current_columns.keys():
                new_column = create_column(val_col, idx_col, col)
                current_columns[(val_col, idx_col, col)] = new_column
                current_columns_to_entities[new_column["id"]] = []
            else:
                new_column = current_columns[(val_col, idx_col, col)]
            used_columns.append(new_column["id"])
            invalidated_columns.append(new_column["id"])
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
                current_columns_to_entities[new_column["id"]].append(
                    old_entity["id"]
                )
                used_col = True
            if not used_cols:
                used_entities.extend(old_entity_in_col)
        # if the column is exclusively in the "after" dataframe
        for col in unique_col_in_df2:
            # control il the column already exist or create it
            val_col = str(df2[col].tolist())
            idx_col = str(df2.index.tolist())
            if (val_col, idx_col, col) not in current_columns.keys():
                new_column = create_column(val_col, idx_col, col)
                generated_columns.append(new_column["id"])
                current_columns[(val_col, idx_col, col)] = new_column
                current_columns_to_entities[new_column["id"]] = []
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
                current_columns_to_entities[new_column["id"]].append(
                    new_entity["id"]
                )

        common_col = set(df1.columns).intersection(set(df2.columns))
        for col in common_col:
            new_column = None
            for idx in df2.index:
                if idx in df1.index:
                    old_value = df1.at[idx, col]
                else:
                    old_value = "Not exist"
                new_value = df2.at[idx, col]
                if old_value != new_value:
                    if (new_value, col, idx) in current_entities:
                        continue
                    # control il the column already exist or create it
                    val_col = str(df2[col].tolist())
                    idx_col = str(df2.index.tolist())
                    if (val_col, idx_col, col) not in current_columns.keys():
                        new_column = create_column(val_col, idx_col, col)
                        generated_columns.append(new_column["id"])
                        current_columns[(val_col, idx_col, col)] = new_column
                        current_columns_to_entities[new_column["id"]] = []
                    if old_value != "Not exist":
                        # same control but for the before df, to get the used columns
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
                            current_columns_to_entities[old_column["id"]] = []
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
                        current_columns_to_entities[old_column["id"]].append(
                            old_entity["id"]
                        )
                    # if gen:
                    generated_entities.append(entity["id"])
                    current_entities[(new_value, col, idx)] = entity
                    current_columns_to_entities[new_column["id"]].append(
                        entity["id"]
                    )

    if get_args().granularity_level == 1 or get_args().granularity_level == 2:
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

    # activities_and_entities[activity['id']] = diff_entities
    # print(activities_and_entities)

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


# neo4j.add_columns(current_columns)
# neo4j.add_derivations(current_derivations)
# neo4j.add_relation_entities_to_column(current_entities_column)
# neo4j.add_derivations_columns(current_derivations)
# neo4j.add_relations(current_relations)
# neo4j.add_relations_columns(current_relations_column)

del current_activities[:]
del current_entities
del current_columns
del current_derivations[:]
del current_entities_column[:]
del current_relations[:]
del current_relations_column[:]
del current_columns_to_entities


session.close()
