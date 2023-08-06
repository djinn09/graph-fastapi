import unittest
from unittest.mock import MagicMock, patch
from app import GremlinConnection, ConnectionPool

class TestGremlinConnection(unittest.TestCase):
    @patch('gremlin_python.driver.client.Client')
    def test_connect(self, mock_client):
        connection = GremlinConnection(None)
        connection.connect()
        self.assertTrue(mock_client.called)

    def test_execute_query(self):
        connection = GremlinConnection(None)
        connection.client = MagicMock()
        result_mock = MagicMock()
        result_mock.all.return_value.result.return_value = [1, 2, 3]
        connection.client.submit.return_value = result_mock

        results = connection.execute_query("g.V().limit(3)")
        self.assertEqual(results, [1, 2, 3])

    @patch('gremlin_python.driver.client.Client')
    def test_close(self, mock_client):
        connection = GremlinConnection(None)
        connection.client = mock_client.return_value
        connection.close()
        connection.client.close.assert_called_once()

class TestConnectionPool(unittest.TestCase):
    def test_create_connection(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=900, max_creation_time=5, circuit_breaker_threshold=3)
        connection = connection_pool.create_connection(None)
        self.assertIsNotNone(connection)

    def test_get_connection(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=900, max_creation_time=5, circuit_breaker_threshold=3)
        with connection_pool.get_connection(None) as connection:
            self.assertIsNotNone(connection)

    def test_adjust_pool_size(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=900, max_creation_time=5, circuit_breaker_threshold=3)
        initial_pool_size = sum(partition.qsize() for partition in connection_pool.partitions.values())
        connection_pool.adjust_pool_size(7)
        final_pool_size = sum(partition.qsize() for partition in connection_pool.partitions.values())
        self.assertEqual(final_pool_size - initial_pool_size, 7)

    def test_adjust_pool_size_reducing(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=900, max_creation_time=5, circuit_breaker_threshold=3)
        initial_pool_size = sum(partition.qsize() for partition in connection_pool.partitions.values())
        connection_pool.adjust_pool_size(1)
        final_pool_size = sum(partition.qsize() for partition in connection_pool.partitions.values())
        self.assertEqual(initial_pool_size - final_pool_size, 1)

    def test_max_creation_time(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=900, max_creation_time=1, circuit_breaker_threshold=3)
        connection = connection_pool.create_connection(None)
        self.assertIsNone(connection)

    def test_circuit_breaker_threshold(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=900, max_creation_time=5, circuit_breaker_threshold=2)
        with self.assertRaises(Exception):
            connection_pool.create_connection(None)
            connection_pool.create_connection(None)
            connection_pool.create_connection(None)

    def test_connection_reuse(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=900, max_creation_time=5, circuit_breaker_threshold=3)
        with connection_pool.get_connection(None) as connection1:
            with connection_pool.get_connection(None) as connection2:
                self.assertIs(connection1, connection2)

    def test_idle_connection_cleanup(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=1, max_creation_time=5, circuit_breaker_threshold=3)
        initial_pool_size = sum(partition.qsize() for partition in connection_pool.partitions.values())
        time.sleep(2)  # Wait for idle connection timeout
        connection_pool.close_idle_connections()
        final_pool_size = sum(partition.qsize() for partition in connection_pool.partitions.values())
        self.assertEqual(final_pool_size, initial_pool_size - 1)

    def test_context_specific_connection(self):
        connection_pool = ConnectionPool(min_connections=2, max_connections=5, timeout=900, max_creation_time=5, circuit_breaker_threshold=3)
        context1_connection = None
        context2_connection = None
        with connection_pool.get_connection("context1") as context1_connection:
            pass
        with connection_pool.get_connection("context2") as context2_connection:
            pass
        self.assertIsNot(context1_connection, context2_connection)

class TestAppRoutes(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()

    def test_execute_query(self):
        response = self.app.post("/execute", json={"query": "g.V().limit(3)"})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)

if __name__ == '__main__':
    unittest.main()
