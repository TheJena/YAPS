from LLM.LLM_activities_descriptor import LLM_activities_descriptor
from LLM.LLM_formatter import LLM_formatter

# from graph.neo4j import Neo4jConnector, Neo4jFactory
# from graph.structure import create_activity
# from graph.constants import *

formatter = LLM_formatter("pipelines/census_pipeline.py", api_key="MY_APY_KEY")

extracted_file = formatter.standardize()

descriptor = LLM_activities_descriptor(extracted_file, api_key="MY_APY_KEY")

activities_description = descriptor.descript()

activities_description_dict = (
    i_do_completely_trust_llms_thus_i_will_evaluate_their_code_on_my_machine(
        activities_description.replace("pipeline = ", "")
    )
)


# neo4j = Neo4jFactory.create_neo4j_queries(uri="bolt://localhost",
#                                                        user="MY_NEO4J_USERNAME",
#                                                        pwd="MY_NEO4J_PASSWORD")
# neo4j.delete_all()
# session = Neo4jConnector().create_session()
#
# current_activities = []
# current_entities = []
# current_columns = []
# current_derivations = []
# current_entities_column = []
# current_relations = []
# current_relations_column = []
#
# for act_name in activities_description_dict.keys():
#     act_context, act_code = activities_description_dict[act_name]
#     activity = create_activity(function_name=act_name, context=act_context, code=act_code)
#     current_activities.append(activity)
#
# # Create constraints in Neo4j
# neo4j.create_constraint(session=session)
#
# # Add activities, entities, derivations, and relations to Neo4j
# neo4j.add_activities(current_activities, session)
#
# pairs = []
# for i in range(len(current_activities)-1):
#     pairs.append({'act_in_id': current_activities[i]['id'], 'act_out_id': current_activities[i+1]['id']})
#
# print(pairs)
# neo4j.add_next_operations(pairs)

# neo4j.add_entities(current_entities)
# neo4j.add_columns(current_columns)
# neo4j.add_derivations(current_derivations)
# neo4j.add_relation_entities_to_column(current_entities_column)
# neo4j.add_derivations_columns(current_derivations)
# neo4j.add_relations(current_relations)
# neo4j.add_relations_columns(current_relations_column)


#
# del current_activities[:]
# del current_entities[:]
# del current_columns[:]
# del current_derivations[:]
# del current_entities_column[:]
# del current_relations[:]
# del current_relations_column[:]
#
#
# session.close()
