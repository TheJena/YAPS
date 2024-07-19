from LLM.LLM_activities_descriptor import LLM_activities_descriptor
from LLM.LLM_formatter import LLM_formatter
from LLM.LLM_activities_used_columns import LLM_activities_used_columns
from graph.neo4j import Neo4jConnector, Neo4jFactory
from graph.structure import *
from utils import *
from column_entity_approach import column_entitiy_vision
from column_approach import column_vision
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
    parser.add_argument(
        "--entity_type_level",
        type=int,
        default=1,
        help="Entity level: 1 for entities and columns and 2 for columns",
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

# Create the activities found by the llm
for act_name in activities_description_dict.keys():
    act_context, act_code = activities_description_dict[act_name]
    activity = create_activity(
        function_name=act_name, context=act_context, code=act_code
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
    ) = column_entitiy_vision(changes, current_activities, get_args())
else:
    current_relations_column, current_columns, derivations_column = (
        column_vision(changes, current_activities, get_args())
    )

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

session.close()
