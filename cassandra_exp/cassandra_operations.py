from cassandra_exp.cassandra_utils import CassandraUtils

# Instance of cassandra utility class
cassandra_utils = CassandraUtils()

key_space_name = 'test_key_space'

# Create keyspace
cassandra_utils.create_key_space(key_space_name)

# connect to keyspace
cassandra_utils.connect_to_keyspace(key_space_name)

# Define a CQL query to create a table students with primary key as studentID
query = '''
create table if not exists students (
   studentID int,
   name text,
   age int,
   marks int,
   primary key(studentID)
);'''

print('\nCreate the table students')
cassandra_utils.execute_query(query)

print('\nCQL query to insert record')
query = "insert into students (studentID, name, age, marks) values (10, 'abc',20, 100);"
cassandra_utils.execute_query(query)

print('\nGet all records from table and display')
query = "select * from students"
rows = cassandra_utils.execute_query(query)
for row in rows:
    print(row)

print('\nFetch records based on primary key column value')
query = "select * from students where studentID=1"
rows = cassandra_utils.execute_query(query)
for row in rows:
    print(row)

print('\nFetch records based on value for a non primary key column, specify "ALLOW FILTERING" ')
print('This method is not recommended as it is inefficient')
query = 'select * from students where age=20 allow filtering'
rows = cassandra_utils.execute_query(query)
for row in rows:
    print(row)

print('\nBatch operations to insert multiple records')
studentlist=[(1,'Juhi',20,100), (2,'dilip',20, 110), (3,'jeevan',24,145)]
queries = []
for student in studentlist:
    queries.append(f"INSERT INTO students (studentID, name, age, marks) VALUES ({student[0]}, \'{student[1]}\', {student[2]}, {student[3]})")
batch_queries = cassandra_utils.get_queries_in_batch(queries)
cassandra_utils.execute_query(batch_queries)


print('\nPrepared statement execution')
query = "INSERT INTO students (studentID, name, age, marks) VALUES (?,?,?,?)"
# returns a prepared statement for which we can bind the values and execute
prepared_statement = cassandra_utils.get_prepared_statement(query)
# Bind values for the prepared statement
query_values_binded = prepared_statement.bind([200,'xyz', 21, 543])
cassandra_utils.execute_query(query_values_binded)



