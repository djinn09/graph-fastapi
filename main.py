# Import the FastAPI, Depends, websockets, os, and logging libraries
from fastapi import FastAPI, Depends
import websockets
import os
import logging

# Import the gremlin_python library and its modules
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# Import the queue and asyncio libraries
import queue
import asyncio

# Import the pydantic library and its modules
from pydantic import BaseModel, Field

# Create a FastAPI app object
app = FastAPI()

# Create a logger object
logger = logging.getLogger(__name__)

# Define the Neptune endpoint and port from environment variables
neptune_endpoint = os.environ.get('NEPTUNE_ENDPOINT')
neptune_port = os.environ.get('NEPTUNE_PORT')

# Define the minimum and maximum number of connections in the pool from environment variables or default values
min_connections = int(os.environ.get('MIN_CONNECTIONS', 5))
max_connections = int(os.environ.get('MAX_CONNECTIONS', 20))

# Define the connection timeout in seconds from environment variables or default value
connection_timeout = int(os.environ.get('CONNECTION_TIMEOUT', 15 * 60))

# Create a queue object to store the connections
connection_pool = queue.Queue(maxsize=max_connections)

# Dictionary to track the usage status of connections
connection_usage = {}

# Get an event loop object
loop = asyncio.get_event_loop()

# Dependency to get a connection from the pool or create a new one if needed
async def get_connection():
    try:
        # Try to get a connection from the queue
        ws = connection_pool.get(block=False)
        connection_usage[ws] = True  # Mark the connection as in use
        return ws
    except queue.Empty:
        # If the queue is empty, create a new connection
        ws = await create_connection()
        connection_usage[ws] = True  # Mark the connection as in use
        return ws

# Dependency to return a connection to the pool or mark it as available
async def return_connection(ws):
    if ws in connection_usage:
        connection_usage[ws] = False  # Mark the connection as available
        try:
            # Put the connection back in the queue without blocking
            connection_pool.put(ws, block=False)
        except queue.Full:
            # If the queue is full, close the connection
            await close_connection(ws)

# Dependency to close a connection
async def close_connection(ws):
    if ws in connection_usage:
        await ws.close()
        del connection_usage[ws]

# Define a function to create a websocket connection with Neptune
async def create_connection():
    ws = await asyncio.wait_for(websockets.connect(f'wss://{neptune_endpoint}:{neptune_port}/gremlin'), timeout=15)
    connection_usage[ws] = True  # Mark the connection as in use
    return ws

# Define a Pydantic model for the input data schema
class QueryInput(BaseModel):
    query: str = Field(..., example="person")

# Define a Pydantic model for the output data schema
class QueryOutput(BaseModel):
    result: list

# Define an endpoint that takes a query input and returns a query output using a connection from the pool
@app.post('/', response_model=QueryOutput)
async def execute_query(input: QueryInput, remote_connection: DriverRemoteConnection = Depends(get_connection)):
    ws = remote_connection._client._transport._ws  # Get the WebSocket object from the remote connection object
    try:
        g = Graph().traversal().withRemote(remote_connection)  # Create a graph traversal object with the remote connection dependency
        result = g.V().hasLabel(input.query).toList()  # Run the query using the input data 
        output = QueryOutput(result=result)  # Create an output object using the output data schema 
        return output  # Return the output object as JSON 
    finally:
        await return_connection(ws)  # Return the connection to the pool

# Background task to periodically check and replace connections that are idle for longer than 15 minutes 
async def check_connection_timeouts():
    while True:
        await asyncio.sleep(60)  # Check every 1 minute 
        current_time = time.time()
        for ws, in_use in connection_usage.items():
            if not in_use and current_time - ws.creation_time > connection_timeout:
                await close_connection(ws)  # Close and delete the idle connection 
                new_ws = await create_connection()  # Create a new connection 
                connection_pool.put(new_ws)  # Put the new connection in the queue 
                new_ws.creation_time = current_time  # Set the creation time of the new connection 

@app.on_event("startup")
async def startup_event():
    # Fill the connection pool with the minimum number of connections
    await fill_connection_pool()
    # Start the background task to check connection timeouts
    loop.create_task(check_connection_timeouts())

@app.on_event("shutdown")
async def shutdown_event():
    # Close all the connections in the pool
    for ws in connection_usage.keys():
        await close_connection(ws)

