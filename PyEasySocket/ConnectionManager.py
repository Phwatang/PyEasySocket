# %%
from asyncio.events import AbstractEventLoop
import concurrent.futures
import asyncio
import aiohttp
from typing import Callable, Any, Dict

# _ProactorBasePipeBasePipeTransport error throws on Windows 10.
# Code below is to silence error.
# https://github.com/aio-libs/aiohttp/issues/4324
from functools import wraps
from asyncio.proactor_events import _ProactorBasePipeTransport
import platform
def silence_event_loop_closed(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as e:
            if str(e) != 'Event loop is closed':
                raise
    return wrapper
if platform.system() == 'Windows':
    # Silence the exception here.
    _ProactorBasePipeTransport.__del__ = silence_event_loop_closed(_ProactorBasePipeTransport.__del__)


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
        self.tasks: Dict[str, asyncio.Task] = {}
        self.url = url
        self.closeTimeout = closeTimeout

    async def start(self):
        """Starts up the connection and submits the necessary coroutines for operation
        
        Reminder: Ensure the event loop its operating on never gets blocked for long.
        (E.g sleep(), input() etc). Otherwise send/receive of the socket will also halt."""
        self.tasks["startup"] = self.eventLoop.create_task(self.__startupRoutine())
    
    async def __startupRoutine(self): 
        # Open/close connection
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.url) as websocket:
                # Place __receiveRoutine and __sendRoutine onto the event loop
                self.tasks["receive"] = self.eventLoop.create_task(self.__receiveRoutine(websocket))
                self.tasks["send"] = self.eventLoop.create_task(self.__sendRoutine(websocket))
                # await for both coroutines to finish
                await self.tasks["send"]
                await self.tasks["receive"]
    async def __receiveRoutine(self, websocket: aiohttp.ClientWebSocketResponse):
        async for msg in websocket:
            if websocket.closed == True:
                break
            # Check for other errors
            if msg.type == aiohttp.WSMsgType.TEXT:
                self.receiveCallback(msg.data)
            else:
                break
    async def __sendRoutine(self, websocket: aiohttp.ClientWebSocketResponse):
        while True:
            msg = await self.sendingQ.get()
            if msg == "EXIT": # Check if close() method has been called
                await websocket.close()
                break
            await websocket.send_str(msg)

    async def close(self):
        """Asynchronously close connection and close all coroutines associated
        with this object"""
        # Doing self.sendingQ.put_nowait("EXIT") could be simpler here?
        self.tasks["exitput"] = asyncio.create_task(self.sendingQ.put("EXIT"))
        await asyncio.gather(*self.tasks.values())

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
        concurrent.futures.wait(futureCloses, return_when=concurrent.futures.ALL_COMPLETED)
        # Close the event loop
        self.eventLoop.call_soon_threadsafe(self.eventLoop.stop)
        # Block till event loop is closed
        self.eventLoopExecution.result()
 
# %%
