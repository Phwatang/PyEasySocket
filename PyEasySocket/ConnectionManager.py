# %%
from asyncio.events import AbstractEventLoop
import concurrent.futures
import ssl
import asyncio
import websockets
from typing import Callable, Any, Dict
# Extension import necessary to properly communicate to some servers
import websockets.extensions.permessage_deflate as deflateExt

sharedSSLContext = ssl.create_default_context()
class EasyAsyncWS:
    """Handle a single websocket connection using coroutines to send/receive.
    To send a message, call .sendingQ.put_nowait method.

    receiveCallback function have a message received parsed in as the only
    parameter.
    
    eventLoop param should be the event loop where start() and close() will be
    called from. If left as default, eventLoop will be asyncio.get_event_loop()"""
    def __init__(self, url: str, receiveCallback: Callable[[Any], None], 
        startingMessage = "", eventLoop: AbstractEventLoop = None,
        closeTimeout = None
    ) -> None:
        # Storing event loop
        if eventLoop is None:
            self.eventLoop = asyncio.get_event_loop()
        else:
            self.eventLoop = eventLoop
        # Create sending queue
        self.sendingQ = asyncio.Queue(loop = self.eventLoop)
        self.receiveCallback = receiveCallback
        if startingMessage != "":
            self.sendingQ.put_nowait(startingMessage)
        # Utility list to store associated running coroutines
        self.tasks = []
        self.url = url
        self.closeTimeout = closeTimeout

    async def start(self):
        """Starts up the connection and submits the necessary coroutines for operation
        
        Reminder: Ensure the event loop its operating on never gets blocked for long.
        (E.g sleep(), input() etc). Otherwise send/receive of the socket will also halt."""
        self.tasks.append(self.eventLoop.create_task(self.__startupRoutine()))
    
    async def __startupRoutine(self): 
        # Open/close connection
        async with websockets.connect(
            self.url, 
            extensions = [deflateExt.ClientPerMessageDeflateFactory()],
            close_timeout = self.closeTimeout
        ) as websocket:
            # Place __receiveRoutine onto the event loop
            self.tasks.append(self.eventLoop.create_task(self.__receiveRoutine(websocket)))
            # __sendRoutine will finish when close() method is called
            await self.__sendRoutine(websocket)
    async def __receiveRoutine(self, websocket):
        # This will automatically exit with an error when websocket has closed
        async for msg in websocket:
            self.receiveCallback(msg)
    async def __sendRoutine(self, websocket):
        while True:
            msg = await self.sendingQ.get()
            if msg == "EXIT": # Check if close() method has been called
                break
            await websocket.send(msg)

    async def close(self):
        """Asynchronously close connection and close all coroutines associated
        with this object"""
        # Doing self.sendingQ.put_nowait("EXIT") could be simpler here?
        self.tasks.append(asyncio.create_task(self.sendingQ.put("EXIT")))
        await asyncio.gather(*self.tasks)

class ConnectionManager:
    """Manages the creation/deletion of EasyAsyncWS websockets in
    a synchronous context.

    All connections execute on the same async event loop running
    on a seperate thread."""
    def __init__(self) -> None:
        self.threadPool = concurrent.futures.ThreadPoolExecutor()
        self.connections: Dict[str, EasyAsyncWS] = {}
        self.eventLoop = asyncio.new_event_loop()
        # Set loop running on seperate thread
        def runLoop():
            asyncio.set_event_loop(self.eventLoop)
            self.eventLoop.run_forever()
        self.eventLoopExecution = self.threadPool.submit(runLoop)

    def addConnection(self, name: str, url: str, 
        receiveCallback: Callable[[Any], None], startingMessage = "",
        closeTimeout = None
    ) -> None:
        """Creates a new websocket connection and runs it in the background.
        
        receiveCallback function have a message received parsed in as the only
        parameter."""
        if name in self.connections:
            print("Error: name argument is already in use")
            return
        # Create the connection object
        self.connections[name] = EasyAsyncWS(
            url = url,
            receiveCallback = receiveCallback,
            startingMessage = startingMessage,
            eventLoop = self.eventLoop,
            closeTimeout = closeTimeout)
        # Submit connection to event loop
        asyncio.run_coroutine_threadsafe(self.connections[name].start(), self.eventLoop)

    def __getitem__(self, name: str) -> EasyAsyncWS:
        return self.connections[name]

    def closeConnection(self, name: str) -> None:
        """Closes websocket connection of a given name."""
        if name in self.connections:
            asyncio.run_coroutine_threadsafe(self.connections[name].close(), self.eventLoop)
    def close(self) -> None:
        """Halts and closes all websocket connections and the background thread."""
        futureCloses = []
        for connection in self.connections.values():
            # closure is a concurrent.Future object, not asyncio.Future
            closure = asyncio.run_coroutine_threadsafe(connection.close(), self.eventLoop)
            futureCloses.append(closure)
        # Block until sockets are closed
        concurrent.futures.wait(futureCloses)
        # Close the event loop
        self.eventLoop.call_soon_threadsafe(self.eventLoop.stop)
        # Block till event loop is closed
        self.eventLoopExecution.result()
 
# %%
