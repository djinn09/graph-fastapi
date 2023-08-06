import os
import logging
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException
from contextlib import contextmanager
import time
import queue
from prometheus_client import start_http_server, Gauge, Histogram
from gremlin_python.driver import client, serializer
from decouple import config

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration from environment variables or .env file
NEPTUNE_ENDPOINT = config('NEPTUNE_ENDPOINT', default='wss://your-neptune-endpoint:8182/gremlin')
MIN_CONNECTIONS = config('MIN_CONNECTIONS', default=2, cast=int)
MAX_CONNECTIONS = config('MAX_CONNECTIONS', default=10, cast=int)
CONNECTION_TIMEOUT = config('CONNECTION_TIMEOUT', default=900, cast=int)
MAX_CREATION_TIME = config('MAX_CREATION_TIME', default=5, cast=int)
CIRCUIT_BREAKER_THRESHOLD = config('CIRCUIT_BREAKER_THRESHOLD', default=10, cast=int)

# Constants for Prometheus metrics
connection_gauge = Gauge("connection_pool_connections", "Number of active connections")
connection_creation_time_histogram = Histogram("connection_creation_time_seconds", "Histogram of connection creation times")
query_execution_time_histogram = Histogram("query_execution_time_seconds", "Histogram of query execution times")
query_execution_error_count = Gauge("query_execution_errors", "Number of query execution errors")




class GremlinConnection:
    """A wrapper class to manage a Gremlin connection."""
    def __init__(self, context_id):
        self.client = None  # The client object for the Gremlin connection
        self.context_id = context_id  # The context ID for the connection
        self.failed_attempts = 0  # The number of failed attempts to connect

    def connect(self):
        """Execute a Gremlin query and return the results."""
        try:
            if self.failed_attempts < 3:  # If there have been less than 3 failed attempts to connect
                # Create a new client object for the Gremlin connection
                self.client = client.Client(NEPTUNE_ENDPOINT, 'g', message_serializer=serializer.GraphSONSerializersV2d0())
                self.failed_attempts = 0  # Reset the failed attempts counter
            else:
                # If there have been 3 or more failed attempts to connect, trigger the circuit breaker
                logger.warning("Circuit breaker triggered. Connection refused.")
                raise Exception("Circuit breaker triggered")
        except Exception as e:
            logger.error("Connection error: %s", str(e))
            self.failed_attempts += 1  # Increment the failed attempts counter
            raise

    def execute_query(self, query: str):
        try:
            if not self.client:  # If there is no client object for the Gremlin connection
                raise Exception("Not connected to database")
            start_time = time.time()  # Record the start time of the query execution
            result_set = self.client.submit(query)  # Submit the query to the Gremlin server
            results = result_set.all().result()  # Retrieve all results from the result set
            execution_time = time.time() - start_time  # Calculate the execution time of the query
            query_execution_time_histogram.observe(execution_time)  # Record the execution time in a histogram
            return results  # Return the results of the query
        except Exception as e:
            logger.error("Query execution error: %s", str(e))
            query_execution_error_count.inc()  # Increment the error count for query execution errors
            raise

    def close(self):
        """Close the Gremlin connection."""
        try:
            if self.client:  # If there is a client object for the Gremlin connection
                self.client.close()  # Close the connection
        except Exception as e:
            logger.error("Error while closing connection: %s", str(e))


class ConnectionPool:
    """A connection pool for managing Gremlin connections."""
    def __init__(self, min_connections: int, max_connections: int, timeout: int, max_creation_time: int, circuit_breaker_threshold: int):
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.timeout = timeout
        self.max_creation_time = max_creation_time
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.connections = queue.Queue()
        self.partitions = {}
        self.failed_connection_attempts = 0

        for _ in range(self.min_connections):
            connection = GremlinConnection(None)
            connection.connect()
            self.connections.put(connection)
            connection_gauge.inc()

    def create_connection(self, context_id):
        """Create a new connection if within the pool size limits."""
        try:
            start_time = time.time()
            connection = GremlinConnection(context_id)
            connection.connect()
            creation_time = time.time() - start_time

            connection_creation_time_histogram.observe(creation_time)

            if creation_time > self.max_creation_time:
                logger.warning("Connection creation exceeded max time: %f seconds", creation_time)
                connection.close()
                return None

            self.connection_gauge.inc()
            self.failed_connection_attempts = 0
            return connection
        except Exception as e:
            logger.error("Error creating connection: %s", str(e))
            self.failed_connection_attempts += 1
            if self.failed_connection_attempts > self.circuit_breaker_threshold:
                logger.warning("Circuit breaker triggered. Too many failed connection attempts.")
                self.failed_connection_attempts = 0
            raise

    def close_idle_connections(self):
        
        now = time.time()
        while not self.connections.empty():
            connection = self.connections.get()
            if now - connection.creation_time > self.timeout:
                connection.close()
                self.connection_gauge.dec()
            else:
                self.connections.put(connection)
                break

    @contextmanager
    def get_connection(self, context_id):
        """Get a connection from the pool or create a new one if necessary."""
        if context_id not in self.partitions:
            self.partitions[context_id] = queue.Queue()

        try:
            connection = self.partitions[context_id].get_nowait()
        except queue.Empty:
            connection = self.create_connection(context_id)

        if connection:
            yield connection
            if time.time() - connection.creation_time > self.timeout:
                connection.close()
            else:
                self.partitions[context_id].put(connection)

    def adjust_pool_size(self, num_connections):
        """Adjust the pool size to the desired value."""
        current_pool_size = sum(partition.qsize() for partition in self.partitions.values())
        if num_connections > current_pool_size:
            diff = num_connections - current_pool_size
            for _ in range(diff):
                connection = self.create_connection(None)
                if connection:
                    self.connections.put(connection)
        elif num_connections < current_pool_size:
            diff = current_pool_size - num_connections
            for _ in range(diff):
                for partition in self.partitions.values():
                    if not partition.empty():
                        connection = partition.get()
                        connection.close()
                    else:
                        break

# Create a connection pool instance with enhanced metrics and monitoring
connection_pool = ConnectionPool(
    min_connections=MIN_CONNECTIONS,
    max_connections=MAX_CONNECTIONS,
    timeout=CONNECTION_TIMEOUT,
    max_creation_time=MAX_CREATION_TIME,
    circuit_breaker_threshold=CIRCUIT_BREAKER_THRESHOLD
)

# Route to execute a Gremlin query
@app.post("/execute")
async def execute_query(query: str, background_tasks: BackgroundTasks, request: Request):
     """
    Execute a Gremlin query using a connection from the connection pool.
    :param query: The Gremlin query to execute.
    :param background_tasks: FastAPI BackgroundTasks for executing tasks in the background.
    :param request: FastAPI Request for obtaining request context.
    :return: The query results.
    """
    context_id = request.headers.get("X-Context-ID")
    if not context_id:
        context_id = "default"
    
    with connection_pool.get_connection(context_id) as connection:
        background_tasks.add_task(connection_pool.close_idle_connections)
        results = connection.execute_query(query)
        return results

# Start Prometheus HTTP server during startup
@app.on_event("startup")
async def startup_event():
    connection_pool.close_idle_connections()
    start_http_server(8000)  # Expose Prometheus metrics on port 8000
