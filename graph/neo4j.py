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

from graph.constants import (
    ACTIVITY_CONSTRAINT,
    ACTIVITY_LABEL,
    BELONGS_RELATION,
    COLUMN_CONSTRAINT,
    COLUMN_LABEL,
    DERIVATION_RELATION,
    ENTITY_CONSTRAINT,
    ENTITY_LABEL,
    GENERATION_RELATION,
    INVALIDATION_RELATION,
    NEXT_RELATION,
    USED_RELATION,
)
from logging import debug, error
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool
from neo4j import GraphDatabase, Session
from typing import List, Optional
from utils import Singleton


@Singleton
class Neo4jConnector:
    """
    Class defining a connector for Neo4j.
    """

    def __init__(self, uri: str, user: str, pwd: str) -> None:
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None

        try:
            self.__driver = GraphDatabase.driver(
                self.__uri, auth=(self.__user, self.__pwd)
            )
        except Exception as e:
            error(f"Failed to create the driver: {e!s}")

    def close(self) -> None:
        """
        Closes the Neo4j driver.
        """

        if self.__driver is not None:
            self.__driver.close()

    def create_session(self, db=None) -> Session:
        """
        Creates a Neo4j session.

        :param db: Optional parameter specifying the database to connect to.
        :return: A Neo4j session.
        """

        return (
            self.__driver.session(database=db)
            if db is not None
            else self.__driver.session()
        )


class Neo4jQueryExecutor:
    """
    Class that executes queries for Neo4j.
    """

    def __init__(self, connector) -> None:
        self.__connector = connector

    def write_transaction(self, query: str) -> None:
        def transaction(tx) -> None:
            tx.run(query)

        with self.__connector.create_session(db=None) as session:
            session.write_transaction(transaction)

    def write_transaction2(self, query: str, batch_size: int = 500):
        def delete_batch(tx, query, batch_size):
            result = tx.run(query, parameters={"batch_size": batch_size})
            return len(list(result))

        with self.__connector.create_session(db=None) as session:
            while True:
                result = session.write_transaction(
                    delete_batch,
                    query,
                    batch_size,
                )
                debug(
                    f"Result of write_transaction2(query=\n{query}\n) "
                    f"= {result!r}"
                )
                if result < batch_size:
                    break

    def query(
        self,
        query: str,
        parameters: dict = None,
        db: str = None,
        session: Session = None,
    ) -> Optional[list]:
        """
        Executes a query. If the provided session is None, it creates
        a new session internally to execute the query.

        :param query: The query to execute.
        :param parameters: Parameters for the query.
        :param db: The database to connect to.
        :param session: An externally created Neo4j session to use for
                        executing the query.
        :return: The query result as a list or None if an error occurred.
        """

        if not self.__connector:
            raise ValueError("Connector not initialized!")

        response = None

        external_session = False
        if session:
            external_session = True

        try:
            if not external_session:
                session = self.__connector.create_session(db=db)
            response = session.run(query, parameters).data()
        except Exception as e:
            error(f"Query failed: {e} {query}")
        finally:
            # Close the session if it was internally created
            if session is not None and not external_session:
                session.close()
        return response

    def insert_data_multiprocess(
        self,
        query: str,
        rows: List[any],
        **kwargs,
    ) -> None:
        """
        Divides the data into batches. Each batch is assigned to a
        process that loads it into Neo4j.
        The method completes when all workers have finished execution.

        :param query: The query to execute.
        :param rows: The rows to load.
        :kwargs: Additional parameters to load.
        """

        pool = Pool(processes=(cpu_count() - 1))
        batch_size = (
            len(rows) // (cpu_count() - 1)
            if len(rows) >= cpu_count() - 1
            else cpu_count()
        )
        batch = 0

        while batch * batch_size < len(rows):
            parameters = {
                "rows": rows[
                    batch * batch_size : (batch + 1) * batch_size
                ]  # noqa
            }
            pool.apply_async(
                self.query,
                args=(query,),
                kwds={"parameters": {**parameters, **kwargs}},
            )
            batch += 1

        pool.close()
        pool.join()


class Neo4jQueries:
    """
    Class containing predefined queries for Neo4j.
    """

    def __init__(self, query_executor):
        self.__query_executor = query_executor

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def create_constraint(self, session=None) -> None:
        """
        Creates constraints for Neo4j nodes.

        :param session: An optional Neo4j session to use for executing
                        the query.
        """

        for query in (
            f"DROP CONSTRAINT {ACTIVITY_CONSTRAINT}",
            f"DROP CONSTRAINT {ENTITY_CONSTRAINT}",
            f"DROP CONSTRAINT {COLUMN_CONSTRAINT}",
        ):
            try:
                self.__query_executor.query(
                    query=query,
                    parameters=None,
                    session=session,
                )
            except Exception as e:
                if "Unable to drop constraint" in str(
                    e
                ) and "No such constraint" in str(e):
                    continue
                raise e

        for query in (
            f"CREATE CONSTRAINT {ACTIVITY_CONSTRAINT} "
            f"FOR (a:{ACTIVITY_LABEL}) REQUIRE a.id IS UNIQUE",
            #
            f"CREATE CONSTRAINT {ENTITY_CONSTRAINT} "
            f"FOR (e:{ENTITY_LABEL}) REQUIRE e.id IS UNIQUE",
            #
            f"CREATE CONSTRAINT {COLUMN_CONSTRAINT} "
            f"FOR (c:{COLUMN_LABEL}) REQUIRE c.id IS UNIQUE",
        ):
            try:
                self.__query_executor.query(
                    query=query,
                    parameters=None,
                    session=session,
                )
            except Exception as e:
                raise e

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def delete_all(self, session=None):
        """
        Deletes all nodes and relationships in the database.

        :param session: An optional Neo4j session to use for executing
                        the query.
        :return: The query result as a list or None if an error occurred.
        """

        query = """
                MATCH (n)
                CALL (n) { WITH n
                DETACH DELETE n
                } IN TRANSACTIONS OF 1000 ROWS;
                """

        debug(query)
        # self.__query_executor.write_transaction2(query)
        self.__query_executor.query(query, parameters=None, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def add_activities(self, activities: List[any], session=None) -> None:
        """
        Adds activities to the database.

        :param activities: The activities to add.
        :param session: An optional Neo4j session to use for executing
                        the query.
        :return: The query result as a list or None if an error occurred.
        """
        query = (
            """
                UNWIND $rows AS row
                CREATE (a:"""
            + ACTIVITY_LABEL
            + """)
                SET a = row
                """
        )
        debug(query)
        self.__query_executor.query(
            query, parameters={"rows": activities}, session=session
        )

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def add_entities(self, entities: List[any]) -> None:
        """
        Adds entities to the database.

        :param entities: The entities to add.
        :return: None
        """
        query = (
            """
                UNWIND $rows AS row
                CREATE (e:"""
            + ENTITY_LABEL
            + """)
                SET e=row
                """
        )
        debug(query)
        self.__query_executor.insert_data_multiprocess(
            query=query,
            rows=entities,
        )

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def add_columns(self, columns: List[any]) -> None:
        """
        Adds entities to the database.

        :param columns: The entities to add.
        :return: None
        """
        query = (
            """
                UNWIND $rows AS row
                CREATE (c:"""
            + COLUMN_LABEL
            + """)
                SET c=row
                """
        )
        debug(query)
        self.__query_executor.insert_data_multiprocess(
            query=query,
            rows=columns,
        )

    def udpate_entities(self, entities: List[any]) -> None:
        """
        Updates entities in the database.

        :param entities: The entities to update.
        :return: None
        """
        query = (
            """
                UNWIND $rows AS row
                MATCH (e:"""
            + ENTITY_LABEL
            + """)
                WHERE e.id = row.id
                SET e=row
                """
        )
        debug(query)
        self.__query_executor.insert_data_multiprocess(
            query=query,
            rows=entities,
        )

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def add_derivations(self, derivations: List[any]) -> None:
        """
        Adds derivations (relationships between entities) to the database.

        :param derivations: The derivations to add.
        :return: None
        """
        query = (
            """
                UNWIND $rows AS row
                MATCH (e1:"""
            + ENTITY_LABEL
            + """ {id: row.gen})
                WITH e1, row
                MATCH (e2:"""
            + ENTITY_LABEL
            + """ {id: row.used})
                MERGE (e1)-[:"""
            + DERIVATION_RELATION
            + """]->(e2)
                """
        )
        debug(query)
        self.__query_executor.insert_data_multiprocess(
            query=query,
            rows=derivations,
        )

    def add_derivations_columns(self, derivations: List[any]) -> None:
        """
        Adds derivations (relationships between entities) to the database.

        :param derivations: The derivations to add.
        :return: None
        """
        query = (
            """
                UNWIND $rows AS row
                MATCH (c1:"""
            + COLUMN_LABEL
            + """ {id: row.gen})
                WITH c1, row
                MATCH (c2:"""
            + COLUMN_LABEL
            + """ {id: row.used})
                MERGE (c1)-[:"""
            + DERIVATION_RELATION
            + """]->(c2)
                """
        )
        debug(query)
        self.__query_executor.insert_data_multiprocess(
            query=query,
            rows=derivations,
        )

    def add_relation_entities_to_column(self, relations: List[any]) -> None:
        """
        Adds relation to the column.

        :param relations: The derivations to add.
        :return: None
        """
        for relation in relations:
            column = relation[0]
            entities = relation[1]

            query = (
                """
                    UNWIND $rows AS row
                    MATCH (e:"""
                + ENTITY_LABEL
                + """ {id: row})
                    WITH e
                    MATCH (a:"""
                + COLUMN_LABEL
                + """ {id: $column})
                    MERGE (e)-[:"""
                + BELONGS_RELATION
                + """]->(a)
                    """
            )
            debug(query)
            self.__query_executor.insert_data_multiprocess(
                query=query, rows=entities, column=column
            )

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def add_relations(self, relations: List[any]) -> None:
        """
        Adds relations (relationships between activities and entities)
        to the database.

        :param relations: The relations to add.
        :return: None
        """
        for relation in relations:
            generated = relation[0]
            used = relation[1]
            invalidated = relation[2]
            same = relation[3]
            act_id = relation[4]

            if same:
                invalidated = used

            query1 = (
                """
                    UNWIND $rows AS row
                    MATCH (e:"""
                + ENTITY_LABEL
                + """ {id: row})
                    WITH e
                    MATCH (a:"""
                + ACTIVITY_LABEL
                + """ {id: $act_id})
                    MERGE (a)-[:"""
                + USED_RELATION
                + """]->(e)
                    """
            )
            query2 = (
                """
                    UNWIND $rows AS row
                    MATCH (e:"""
                + ENTITY_LABEL
                + """ {id: row})
                    WITH e
                    MATCH (a:"""
                + ACTIVITY_LABEL
                + """ {id: $act_id})
                    MERGE (e)-[:"""
                + GENERATION_RELATION
                + """]->(a)
                    """
            )
            query3 = (
                """
                    UNWIND $rows AS row
                    MATCH (e:"""
                + ENTITY_LABEL
                + """ {id: row})
                    WITH e
                    MATCH (a:"""
                + ACTIVITY_LABEL
                + """ {id: $act_id})
                    MERGE (e)-[:"""
                + INVALIDATION_RELATION
                + """]->(a)
                    """
            )

            debug(query1)
            debug(query2)
            debug(query3)

            self.__query_executor.insert_data_multiprocess(
                query=query1, rows=used, act_id=act_id
            )
            self.__query_executor.insert_data_multiprocess(
                query=query2, rows=generated, act_id=act_id
            )
            self.__query_executor.insert_data_multiprocess(
                query=query3, rows=invalidated, act_id=act_id
            )

    def add_relations_columns(self, relations: List[any]) -> None:
        """
        Adds relations (relationships between activities and entities)
        to the database.

        :param relations: The relations to add.
        :return: None
        """
        for relation in relations:
            generated = relation[0]
            used = relation[1]
            invalidated = relation[2]
            same = relation[3]
            act_id = relation[4]

            if same:
                invalidated = used

            query1 = (
                """
                    UNWIND $rows AS row
                    MATCH (c:"""
                + COLUMN_LABEL
                + """ {id: row})
                    WITH c
                    MATCH (a:"""
                + ACTIVITY_LABEL
                + """ {id: $act_id})
                    MERGE (a)-[:"""
                + USED_RELATION
                + """]->(c)
                    """
            )
            query2 = (
                """
                    UNWIND $rows AS row
                    MATCH (c:"""
                + COLUMN_LABEL
                + """ {id: row})
                    WITH c
                    MATCH (a:"""
                + ACTIVITY_LABEL
                + """ {id: $act_id})
                    MERGE (c)-[:"""
                + GENERATION_RELATION
                + """]->(a)
                    """
            )
            query3 = (
                """
                    UNWIND $rows AS row
                    MATCH (c:"""
                + COLUMN_LABEL
                + """ {id: row})
                    WITH c
                    MATCH (a:"""
                + ACTIVITY_LABEL
                + """ {id: $act_id})
                    MERGE (c)-[:"""
                + INVALIDATION_RELATION
                + """]->(a)
                    """
            )

            debug(query1)
            debug(query2)
            debug(query3)

            self.__query_executor.insert_data_multiprocess(
                query=query1, rows=used, act_id=act_id
            )
            self.__query_executor.insert_data_multiprocess(
                query=query2, rows=generated, act_id=act_id
            )
            self.__query_executor.insert_data_multiprocess(
                query=query3, rows=invalidated, act_id=act_id
            )

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def add_next_operations(
        self,
        next_operations: List[any],
        session=None,
    ) -> None:
        """
        Adds relationships between activities representing the order
        in which they occur.

        :param next_operations: The next operations to add.
        :param session: An optional Neo4j session to use for executing
                        the query.
        :return: None
        """
        query = (
            """
                UNWIND $next_operations AS next_operation
                MATCH (a1:"""
            + ACTIVITY_LABEL
            + """ {id: next_operation.act_in_id})
                WITH a1, next_operation
                MATCH (a2:"""
            + ACTIVITY_LABEL
            + """ {id: next_operation.act_out_id})
                MERGE (a1)-[:"""
            + NEXT_RELATION
            + """]->(a2)
                """
        )

        debug(query)

        self.__query_executor.query(
            query,
            parameters={"next_operations": next_operations},
            session=session,
        )

    def create_useful_indexes(self, session=None) -> None:
        query = f"""
                CREATE INDEX entity_index IF NOT EXISTS
                FOR (n:{ENTITY_LABEL})
                ON (n.id)
                """
        self.__query_executor.query(
            query=query,
            parameters=None,
            session=session,
        )

        query = """
                CREATE LOOKUP INDEX rel_type_lookup_index
                FOR()-[r]-() ON EACH type(r)
                """
        self.__query_executor.query(
            query=query,
            parameters=None,
            session=session,
        )

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def all_transformations(self, session=None):
        query = f"""
                MATCH (a:{ACTIVITY_LABEL})
                RETURN a
                """

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def why_provenance(self, entity_id: str, session=None):
        query = (
            """
                MATCH (e:"""
            + ENTITY_LABEL
            + '''{id:"'''
            + entity_id
            + """"})-[:"""
            + DERIVATION_RELATION
            + """]->(m:"""
            + ENTITY_LABEL
            + """)
                RETURN e,m
                """
        )

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def how_provenance(self, entity_id: str, session=None):
        query = (
            """
                MATCH (e:"""
            + ENTITY_LABEL
            + '''{id:"'''
            + entity_id
            + """"})-[:"""
            + DERIVATION_RELATION
            + """]->(m:"""
            + ENTITY_LABEL
            + """)
                MATCH (m:"""
            + ENTITY_LABEL
            + """)-[]-(a:"""
            + ACTIVITY_LABEL
            + """)
                return e,m,a
                """
        )

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def dataset_level_feature_operation(self, feature: str, session=None):
        query = f"""
                MATCH (a:{ACTIVITY_LABEL})
                WHERE "{feature}" IN a.used_features
                RETURN a
                """

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def record_operation(self, index: str, session=None):
        query = f"""
                MATCH (e:{ENTITY_LABEL})<-[:{USED_RELATION}]-(a:{ACTIVITY_LABEL})
                WHERE {index} = e.index
                RETURN DISTINCT a
                """  # noqa

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def item_level_feature_operation(self, entity_id: str, session=None):
        query = (
            """
                MATCH (e:"""
            + ENTITY_LABEL
            + '''{id:"'''
            + entity_id
            + """"})-[]-(a:"""
            + ACTIVITY_LABEL
            + """)
                RETURN e,a
                """
        )
        debug(query)
        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def item_invalidation(self, entity_id: str, session=None):
        query = (
            """
                MATCH (e:"""
            + ENTITY_LABEL
            + '''{id:"'''
            + entity_id
            + """"})-[:"""
            + INVALIDATION_RELATION
            + """]->(a:"""
            + ACTIVITY_LABEL
            + """)
                RETURN e,a
                """
        )

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def feature_invalidation(self, feature: str, session=None):
        query = f"""
                MATCH (e:{ENTITY_LABEL})-[:{INVALIDATION_RELATION}]->(a:{ACTIVITY_LABEL})
                WHERE "{feature}" IN a.used_features
                RETURN DISTINCT a
                """  # noqa

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def record_invalidation(self, index: str, session=None):
        query = f"""
                MATCH (e:{ENTITY_LABEL})-[:{INVALIDATION_RELATION}]->(a:{ACTIVITY_LABEL})
                WHERE {index} = e.index AND a.deleted_records = true
                RETURN DISTINCT a
        """  # noqa

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def record_history(self, index: str, session=None):
        query = f"""
                MATCH (e:{ENTITY_LABEL})-[r:{DERIVATION_RELATION}*1..]-(m:{ENTITY_LABEL})
                WHERE {index} = e.index
                RETURN DISTINCT e,r,m
                """  # noqa

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def item_history(self, entity_id: str, session=None):
        query = (
            """
                MATCH p=(e:"""
            + ENTITY_LABEL
            + '''{id:"'''
            + entity_id
            + """"})-[r:"""
            + DERIVATION_RELATION
            + """*1..]-(m:"""
            + ENTITY_LABEL
            + """)
                RETURN DISTINCT e,r,m
                """
        )

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def get_random_nodes(self, label: str, limit: int = 3, session=None):
        query = f"""
                MATCH (n:{label})
                WITH n, rand() AS random
                ORDER BY random
                LIMIT {limit}
                RETURN n
                """

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def dataset_spread(self, session=None):
        """
        returns number of invalidated and new entities for each activity -> ?
        """
        query = f"""
                MATCH (e:{ENTITY_LABEL})-[r]->(a:{ACTIVITY_LABEL})
                RETURN a, type(r) AS t, count(*) AS c
                """

        debug(query)

        return self.__query_executor.query(query, session=session)

    # @timing(log_file=NEO4j_QUERY_EXECUTION_TIMES)
    def feature_spread(self, feature: str, session=None):
        """
        returns number of invalidated and new entities for each
        activity that operates on feature -> return activity with max inv
        and one with max new?
        """
        query = f"""
                MATCH (e:{ENTITY_LABEL})-[r]->(a:{ACTIVITY_LABEL})
                WHERE "{feature}" IN a.used_features
                RETURN a, TYPE(r) AS t, COUNT(*) AS c
                """

        debug(query)

        return self.__query_executor.query(query, session=session)


class Neo4jFactory:
    def __init__(self):
        pass

    @staticmethod
    def create_neo4j_queries(uri: str, user: str, pwd: str) -> Neo4jQueries:
        """
        Creates Neo4jQueries object for executing queries on Neo4j.

        :param uri: The URI of the Neo4j database.
        :param user: The username for accessing the Neo4j database.
        :param pwd: The password for accessing the Neo4j database.
        :return: A Neo4jQueries object.
        """

        connector = Neo4jConnector(uri, user, pwd)
        query_executor = Neo4jQueryExecutor(connector)
        queries = Neo4jQueries(query_executor)
        return queries
