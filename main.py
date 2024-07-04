from LLM.LLM_activities_descriptor import LLM_activities_descriptor
from LLM.LLM_formatter import LLM_formatter
from graph.neo4j import Neo4jConnector, Neo4jFactory
from graph.structure import *
from extracted_code import run_pipeline
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
        default="datasets/census.csv",
        help="Relative path to the dataset file",
    )
    parser.add_argument(
        "--frac", type=float, default=0.1, help="Sampling fraction [0.0 - 1.0]"
    )

    return parser.parse_args()


formatter = LLM_formatter("pipelines/census_pipeline.py", api_key="MY_APY_KEY")
extracted_file = formatter.standardize()
descriptor = LLM_activities_descriptor(extracted_file, api_key="MY_APY_KEY")

activities_description = descriptor.descript()
activities_description_dict = (
    i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
        activities_description.replace("pipeline_operations = ", "")
    )
)
neo4j = Neo4jFactory.create_neo4j_queries(
    uri="bolt://localhost", user="MY_NEO4J_USERNAME", pwd="MY_NEO4J_PASSWORD"
)
neo4j.delete_all()
session = Neo4jConnector().create_session()
tracker = ProvenanceTracker(save_on_neo4j=True)
run_pipeline(args=get_args(), tracker=tracker)

# map of all the df before and after the operations
changes = tracker.changes

current_activities = []
current_entities = {}
current_columns = []
current_derivations = []
current_entities_column = []
current_relations = []
current_relations_column = []

# to create the activities find by the llm
for act_name in activities_description_dict.keys():
    act_context, act_code = activities_description_dict[act_name]
    activity = create_activity(
        function_name=act_name, context=act_context, code=act_code
    )
    current_activities.append(activity)

# find the differnce of the df and create the entities
activities_and_entities = {}  # map of the entities modified by the activity
generated_entities = []
used_entities = []
invalidated_entities = []

for act in changes.keys():
    if act == 0:
        continue
    activity = current_activities[act - 1]
    df1 = changes[act]["before"]
    df2 = changes[act]["after"]
    # Initialize an empty list to store the differences
    diff_entities = []
    # Iterate over the columns and rows to find differences
    for col in df2.columns:
        for idx in df2.index:
            if idx in df2.index and col in df2.columns:
                if idx in df1.index and col in df1.columns:
                    old_value = df1.at[idx, col]
                else:
                    old_value = "Not exist"
                new_value = df2.at[idx, col]
                if old_value != new_value:
                    old_entity = None
                    if (old_value, col, idx) in current_entities.keys():
                        old_entity = current_entities[(old_value, col, idx)]
                    else:
                        old_entity = create_entity(old_value, col, idx)
                        current_entities[(old_value, col, idx)] = old_entity
                    entity = create_entity(new_value, col, idx)
                    # diff_entities.append({
                    #     'old_id_entity': old_entity['id'],
                    #     'new_id_entity': entity['id']
                    # })
                    current_entities[(new_value, col, idx)] = entity
    # activities_and_entities[activity['id']] = diff_entities
    # print(activities_and_entities)

# Create constraints in Neo4j
neo4j.create_constraint(session=session)

# Add activities, entities, derivations, and relations to Neo4j
neo4j.add_activities(current_activities, session)
neo4j.add_entities(list(current_entities.values()))

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
del current_entities[:]
del current_columns[:]
del current_derivations[:]
del current_entities_column[:]
del current_relations[:]
del current_relations_column[:]


session.close()
