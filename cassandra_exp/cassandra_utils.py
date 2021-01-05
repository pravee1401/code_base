from io import StringIO
from typing import List

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement


class CassandraUtils:

    def __init__(self):
        """
        Interaction with Cassandra database is done with Cluster object.
        Initializing it without any arguments so that it tries to connect to localhost and default port
        where cassandra_exp would be running
        """
        self.cluster = Cluster()

        # Get the session through which transactions like insert/update etc can be performed
        # keyspace argument can be provided for which the session can connect to here
        self.session = self.cluster.connect()

    def create_key_space(self, key_space_name: str):
        """
        Create key space with provided name for keyspace.
        Key space is a data container similar to a database in RDBMS. It is also called a namespace that contains
        related data, something similar to tables in RDBMS.
        :param key_space_name:
        :return:
        """
        query = StringIO()
        query.write(" create keyspace if not exists ")
        query.write(key_space_name)
        query.write(" with replication = {'class': 'SimpleStrategy',  'replication_factor' : 3 };")
        self.session.execute(query.getvalue())

    def connect_to_keyspace(self, keyspace):
        # Connect to keyspace and set the session to it
        self.session = self.cluster.connect(keyspace)

    def execute_query(self, cql_query):
        """
        Executes the given CQL (Cassandra Language Query) or batch queries or prepared statement,
        using cassandra session and returns results of the query executed
        :param cql_query:
        :return:
        """
        return self.session.execute(cql_query)

    @staticmethod
    def get_queries_in_batch(queries: List[str]):
        """
        Creates SimpleStatement out of each query in the query list provided
        Adds or batches these individual queries into BatchStatement and returns it
        These queries can be executed in a batch using BatchStatement
        :param queries:
        :return:
        """
        batch_queries = BatchStatement()
        for query in queries:
            simple_statement = SimpleStatement(query)
            batch_queries.add(simple_statement)
        return batch_queries

    def get_prepared_statement(self, query: str):
        """
        Return prepared statement for the query provided.
        Prepare statement is like a parameterized query. It is saved by cassandra for later use.
        An instance of PreparedStatement is returned.
        Subsequently it only needs to send the values of parameters to bind.
        :param query:
        :return:
        """
        return self.session.prepare(query)