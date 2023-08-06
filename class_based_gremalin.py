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

app = FastAPI()  # Create a new FastAPI app instance

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set the logging level to INFO
logger = logging.getLogger(__name__)  # Get a logger instance for this module

# Load configuration from environment variables or .env file
NEPTUNE_ENDPOINT = config('NEPTUNE_ENDPOINT', default='wss://your-neptune-endpoint:8182/gremlin')  # The endpoint for the Neptune database
MIN_CONNECTIONS = config('MIN_CONNECTIONS', default=2, cast=int)  # The minimum number of connections in the pool
MAX_CONNECTIONS = config('MAX_CONNECTIONS', default=10, cast=int)  # The maximum number of connections in the pool
CONNECTION_TIMEOUT = config('CONNECTION_TIMEOUT', default=900, cast=int)  # The timeout for idle connections
MAX_CREATION_TIME = config('MAX_CREATION_TIME', default=5, cast=int)  # The maximum time allowed for creating a new connection
CIRCUIT_BREAKER_THRESHOLD = config('CIRCUIT_BREAKER_THRESHOLD', default=10, cast=int)  # The threshold for triggering the circuit breaker

# Constants for Prometheus metrics
connection_gauge = Gauge("connection_pool_connections", "Number of active connections")  # A gauge to track the number of active connections in the pool
connection_creation_time_histogram = Histogram("connection_creation_time_seconds", "Histogram of connection creation times")  # A histogram to track the distribution of connection creation times
query_execution_time_histogram = Histogram("query_execution_time_seconds", "Histogram of query execution times")  # A histogram to track the distribution of query execution times
query_execution_error_count = Gauge("query_execution_errors", "Number of query execution errors")  # A gauge to track the number of query execution errors that have occurred



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
        self.min_connections = min_connections  # The minimum number of connections in the pool
        self.max_connections = max_connections  # The maximum number of connections in the pool
        self.timeout = timeout  # The timeout for idle connections
        self.max_creation_time = max_creation_time  # The maximum time allowed for creating a new connection
        self.circuit_breaker_threshold = circuit_breaker_threshold  # The threshold for triggering the circuit breaker
        self.connections = queue.Queue()  # A queue to store the available connections
        self.partitions = {}  # A dictionary to store the connections by context ID
        self.failed_connection_attempts = 0  # The number of failed attempts to create a new connection

        # Create the minimum number of connections and add them to the pool
        for _ in range(self.min_connections):
            connection = GremlinConnection(None)
            connection.connect()
            self.connections.put(connection)
            connection_gauge.inc()

    def create_connection(self, context_id):
        """Create a new connection if within the pool size limits."""
        try:
            start_time = time.time()  # Record the start time of the connection creation
            connection = GremlinConnection(context_id)  # Create a new GremlinConnection object
            connection.connect()  # Connect to the Gremlin server
            creation_time = time.time() - start_time  # Calculate the creation time of the connection

            connection_creation_time_histogram.observe(creation_time)  # Record the creation time in a histogram

            if creation_time > self.max_creation_time:  # If the creation time exceeded the maximum allowed time
                logger.warning("Connection creation exceeded max time: %f seconds", creation_time)
                connection.close()  # Close the connection
                return None

            self.connection_gauge.inc()  # Increment the connection gauge
            self.failed_connection_attempts = 0  # Reset the failed connection attempts counter
            return connection
        except Exception as e:
            logger.error("Error creating connection: %s", str(e))
            self.failed_connection_attempts += 1  # Increment the failed connection attempts counter
            if self.failed_connection_attempts > self.circuit_breaker_threshold:  # If the failed attempts exceeded the threshold
                logger.warning("Circuit breaker triggered. Too many failed connection attempts.")
                self.failed_connection_attempts = 0  # Reset the failed connection attempts counter
            raise

    def close_idle_connections(self):
        """Close idle connections that have exceeded the timeout."""
        now = time.time()  # Get the current time
        while not self.connections.empty():  # While there are still connections in the pool
            connection = self.connections.get()  # Get a connection from the pool
            if now - connection.creation_time > self.timeout:  # If the idle time of the connection exceeded the timeout
                connection.close()  # Close the connection
                self.connection_gauge.dec()  # Decrement the connection gauge
            else:
                self.connections.put(connection)  # Put the connection back in the pool if it is still valid
                break

    @contextmanager
    def get_connection(self, context_id):
        """Get a connection from the pool or create a new one if necessary."""
        if context_id not in self.partitions:  # If there is no partition for this context ID yet
            self.partitions[context_id] = queue.Queue()  # Create a new partition for this context ID

        try:
            connection = self.partitions[context_id].get_nowait()  # Try to get a connection from this partition without waiting
        except queue.Empty:  # If there are no available connections in this partition
            connection = self.create_connection(context_id)  # Create a new connection

        if connection:  # If a valid connection was obtained or created
            yield connection  # Yield it to be used by the caller

            if time.time() - connection.creation_time > self.timeout:  # If this idle time of this returned exceeded the timeout 
                connection.close()   # Close it 
            else:
                self.partitions[context_id].put(connection)   # Put it back into its partition 

    def adjust_pool_size(self, num_connections):
        """Adjust the pool size to the desired value."""
        current_pool_size = sum(partition.qsize() for partition in self.partitions.values())   # Calculate current pool size 
        if num_connections > current_pool_size:   # If desired size is greater than current size 
            diff = num_connections - current_pool_size   # Calculate difference 
            for _ in range(diff):   # For each difference 
                connection = self.create_connection(None)   # Create a new connection 
                if connection:   # If connection is valid 
                    self.connections.put(connection)   # Put it into the pool 
        elif num_connections < current_pool_size:   # If desired size is less than current size 
            diff = current_pool_size - num_connections   # Calculate difference 
            for _ in range(diff):   # For each difference 
                for partition in self.partitions.values():   # For each partition 
                    if not partition.empty():   # If partition is not empty 
                        connection = partition.get()   # Get a connection from the partition 
                        connection.close()   # Close the connection 
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

@app.on_event("shutdown")
async def shutdown_event():
    connection_pool.close_all_connections()
