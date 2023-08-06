from fastapi import FastAPI, BackgroundTasks, Depends
import asyncio
import websockets
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import queue
import time

app = FastAPI()

# Neptune Configuration
neptune_endpoint = 'your-neptune-endpoint'
neptune_port = 8182

# Define the minimum and maximum number of connections in the pool
min_connections = 5
max_connections = 20

# Connection timeout in seconds
connection_timeout = 15 * 60

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

# Define a function to execute a Gremlin query
async def execute_query(query: str):
    ws = await get_connection()
    remote_connection = DriverRemoteConnection(ws, 'g')
    try:
        g = Graph().traversal().withRemote(remote_connection)
        result = g.V().hasLabel(query).toList()  # Modify the query as needed
        return result
    finally:
        await return_connection(ws)

# Background task to periodically check and replace connections
async def check_connection_timeouts():
    while True:
        await asyncio.sleep(60)  # Check every 1 minute
        current_time = time.time()
        for ws, in_use in connection_usage.items():
            if not in_use and current_time - ws.creation_time > connection_timeout:
                await close_connection(ws)
                new_ws = await create_connection()
                connection_pool.put(new_ws)
                new_ws.creation_time = current_time

@app.on_event("startup")
async def startup_event():
    # Fill the connection pool with the minimum number of connections
    await fill_connection_pool()
    # Start the background task to check connection timeouts
    loop.create_task(check_connection_timeouts())

@app.get('/')
async def execute_gremlin_query(query: str):
    result = await execute_query(query)
    return {"result": result}
