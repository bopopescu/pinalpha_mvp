from neo4jDatabase.dbconnection import neo4jConnection

neo4j_connection = neo4jConnection("bolt://52.77.247.161:7687", "neo4j", "Pin@Alpha123")
print(neo4j_connection.insert_person("bijay", 1986))
print(neo4j_connection.get_n_people(10))