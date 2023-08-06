import os
import logging
from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException, status, BackgroundTasks
from fastapi.routing import Request, APIRouter
from pydantic import BaseModel
from contextlib import contextmanager
import time
import queue
from prometheus_client import start_http_server, Gauge, Histogram
from gremlin_python.driver import client, serializer
from decouple import config  # Import config from python-decouple

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



# Define Prometheus metrics
connection_gauge = Gauge("connection_pool_connections", "Number of active connections")
connection_creation_time_histogram = Histogram("connection_creation_time_seconds", "Histogram of connection creation times")
query_execution_time_histogram = Histogram("query_execution_time_seconds", "Histogram of query execution times")
query_execution_error_count = Gauge("query_execution_errors", "Number of query execution errors")


# Gremlin Python connection class
class GremlinConnection:
    def __init__(self, context_id):
        self.client = None
        self.context_id = context_id
        self.failed_attempts = 0  # Number of failed attempts
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.failed_connection_attempts = 0  # Count of failed connection attempts

    def connect(self):
        try:
            # Simulate connection
            if self.failed_attempts < 3:  # Implement circuit breaker logic here
                self.client = client.Client(NEPTUNE_ENDPOINT, 'g', message_serializer=serializer.GraphSONSerializersV2d0())
                self.failed_attempts = 0  # Reset failed attempts on successful connection
            else:
                logger.warning("Circuit breaker triggered. Connection refused.")
                raise Exception("Circuit breaker triggered")
        except Exception as e:
            logger.error("Connection error: %s", str(e))
            self.failed_attempts += 1
            raise

    # def execute_query(self, query: str):
    #     try:
    #         if not self.client:
    #             raise Exception("Not connected to database")
    #         result_set = self.client.submit(query)
    #         results = result_set.all().result()
    #         return results
    #     except Exception as e:
    #         logger.error("Query execution error: %s", str(e))
    #         raise
    
    def execute_query(self, query: str):
        try:
            if not self.client:
                raise Exception("Not connected to database")
            start_time = time.time()
            result_set = self.client.submit(query)
            results = result_set.all().result()
            execution_time = time.time() - start_time
            query_execution_time_histogram.observe(execution_time)
            return results
        except Exception as e:
            logger.error("Query execution error: %s", str(e))
            query_execution_error_count.inc()
            raise
    
    def close(self):
        try:
            if self.client:
                self.client.close()
        except Exception as e:
            logger.error("Error while closing connection: %s", str(e))

# Enhanced connection pool with request context, partitioning, circuit breaker, and graceful recovery
class ConnectionPool:
    def __init__(self, min_connections: int, max_connections: int, timeout: int, max_creation_time: int, circuit_breaker_threshold: int):
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.timeout = timeout
        self.max_creation_time = max_creation_time
        self.connections = queue.Queue()
        self.connection_gauge = connection_gauge
        self.partitions = {}

        # Initialize the connection pool with minimum connections
        for _ in range(self.min_connections):
            connection = GremlinConnection(None)
            connection.connect()
            self.connections.put(connection)
            self.connection_gauge.inc()

    def create_connection(self, context_id):
        try:
            start_time = time.time()
            connection = GremlinConnection(context_id)
            connection.connect()
            creation_time = time.time() - start_time
    
            # Record connection creation time in histogram
            connection_creation_time_histogram.observe(creation_time)
            # Check if connection creation time exceeds the limit
            if creation_time > self.max_creation_time:
                logger.warning("Connection creation exceeded max time: %f seconds", creation_time)
                connection.close()
                return None
            
            # Increment connection gauge and reset circuit breaker on success
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
        if context_id not in self.partitions:
            self.partitions[context_id] = queue.Queue()

        try:
            connection = self.partitions[context_id].get_nowait()
        except queue.Empty:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Connection pool exhausted")

        # Check if connection is idle for too long
        if time.time() - connection.creation_time > self.timeout:
            connection.close()
            connection = self.create_connection(context_id)

        if connection:
            yield connection
            # Check if connection is idle for too long after usage
            if time.time() - connection.creation_time > self.timeout:
                connection.close()
            else:
                self.partitions[context_id].put(connection)

    def adjust_pool_size(self, num_connections):
        current_pool_size = sum(partition.qsize() for partition in self.partitions.values())
        if num_connections > current_pool_size:
            # Increase pool size by creating new connections
            diff = num_connections - current_pool_size
            for _ in range(diff):
                connection = self.create_connection(None)  # Create a connection without context initially
                if connection:
                    self.connections.put(connection)
        elif num_connections < current_pool_size:
            # Decrease pool size by closing excess connections
            diff = current_pool_size - num_connections
            for _ in range(diff):
                for partition in self.partitions.values():
                    if not partition.empty():
                        connection = partition.get()
                        connection.close()
                        self.connection_gauge.dec()

    def recycle_connections(self, context_id, num_connections):
        if context_id not in self.partitions:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Partition not found")
        
        partition = self.partitions[context_id]

        # Recycle specified number of connections by closing and replacing
        for _ in range(num_connections):
            connection = partition.get()
            connection.close()
            self.connection_gauge.dec()
            new_connection = self.create_connection(context_id)
            if new_connection:
                partition.put(new_connection)

    def close_all_connections(self):
        # Close all connections during shutdown
        for partition in self.partitions.values():
            while not partition.empty():
                connection = partition.get()
                connection.close()
                self.connection_gauge.dec()

    def get_request_context(self, request: Request):
        # Extract and return the request context
        # This can be customized based on your application's requirements
        return {'context_id': request.headers.get('X-Context-Id', 'default')}

# Create a connection pool instance with configuration from environment variables
connection_pool = ConnectionPool(
    min_connections=MIN_CONNECTIONS,
    max_connections=MAX_CONNECTIONS,
    timeout=CONNECTION_TIMEOUT,
    max_creation_time=MAX_CREATION_TIME,
    circuit_breaker_threshold=10  # Adjust the threshold based on your needs

)

# Request model
class QueryInput(BaseModel):
    query: str

# # Enhanced route to execute a query using a connection from the pool with context
# @app.post("/execute_query")
# async def execute_query(
#     query_input: QueryInput, 
#     context: dict = Depends(connection_pool.get_request_context),
#     connection: GremlinConnection = Depends(connection_pool.get_connection)
# ):
#     try:
#         result = connection.execute_query(query_input.query)
#         return {"result": result}
#     except Exception as e:
#         logger.error("Query execution error: %s", str(e))
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Query execution error")


# Route to execute a Gremlin query
@app.post("/execute")
async def execute_query(query: str, background_tasks: BackgroundTasks, request: Request):
    context_id = request.headers.get("X-Context-ID")
    if not context_id:
        context_id = "default"
    
    with connection_pool.get_connection(context_id) as connection:
        background_tasks.add_task(connection_pool.close_idle_connections)
        results = connection.execute_query(query)
        return results



# Enhanced dynamic scaling route to adjust the connection pool size based on demand and context
@app.post("/dynamic_scaling/{num_connections}")
async def dynamic_scaling(num_connections: int, context: dict = Depends(connection_pool.get_request_context)):
    if num_connections < connection_pool.min_connections or num_connections > connection_pool.max_connections:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid number of connections")
    connection_pool.adjust_pool_size(num_connections)
    return {"message": f"Connection pool size adjusted to {num_connections}"}

# Enhanced connection recycling route to recycle specified number of connections for a context
@app.post("/recycle_connections/{num_connections}")
async def recycle_connections(num_connections: int, context: dict = Depends(connection_pool.get_request_context)):
    if num_connections <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid number of connections")
    connection_pool.recycle_connections(context['context_id'], num_connections)
    return {"message": f"{num_connections} connections recycled"}

# Enhanced event handler to replace idle connections during startup
@app.on_event("startup")
async def startup_event():
    logger.info("Initializing connection pool...")
    connection_pool.close_idle_connections()
    start_http_server(8000)
    # You can put additional initialization code here

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Closing all connections...")
    connection_pool.close_all_connections()
