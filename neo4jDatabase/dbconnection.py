from neo4j import GraphDatabase


class neo4jConnection(object):

    def __init__(self, uri, user, password):
        # "bolt://18.234.47.145:7687"
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def print_greeting(self, message):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _get_n_peoples(tx, n):
        result = tx.run("MATCH (people:Person) RETURN people.name ORDER BY people.name LIMIT  {n}", n=n)
        return list(result)

    def get_n_people(self, n):
        with self._driver.session() as session:
            result = session.read_transaction(self._get_n_peoples, n)
            return result

    @staticmethod
    def _insert_person(tx, name, born):
        result = tx.run("CREATE(Keanu: Person{name: {name}, born: {born}}) return Keanu.name", name=name, born=born)
        return list(result)

    def insert_person(self, name, born):
        with self._driver.session() as session:
            result = session.write_transaction(self._insert_person, name, born)
            return result
