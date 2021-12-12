# %%
from asyncio.events import AbstractEventLoop
import concurrent.futures
import asyncio
import aiohttp
from typing import Callable, Any, Dict, List

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


class EasyAsyncBase:
    """Base class to remove code duplication for following classes"""
    def __init__(self, url: str, receiveCallback: Callable[[Any], None],
        eventLoop: AbstractEventLoop = None, startingMessage: str = ""
    ) -> None:
        # Storing event loop
        if eventLoop is None:
            self.eventLoop = asyncio.get_event_loop()
        else:
            self.eventLoop = eventLoop
        # Utility list to store associated running coroutines
        self.tasks: List[asyncio.Task] = []
        # Store other variables
        self.url = url
        self.receiveCallback = receiveCallback
        self.startingMessage = startingMessage

class EasyAsyncHTTP(EasyAsyncBase): 
    """Handle a single http get/post request using coroutines to send/receive.
    
    if startingMessage is default, then it is a get request. Otherwise, it 
    is a post request.

    receiveCallback function will have the message received parsed in as the only
    parameter.
    
    eventLoop param should be the event loop where start() will be
    called from. If left as none, eventLoop will be asyncio.get_event_loop()"""
    def __init__(self, url: str, receiveCallback: Callable[[Any], None],
        eventLoop: AbstractEventLoop = None, startingMessage: str = ""
    ) -> None:
        super().__init__(
            url = url,
            receiveCallback = receiveCallback,
            eventLoop = eventLoop,
            startingMessage = startingMessage
        )

    async def start(self) -> None:
        """Submits the coroutine necessary for the http request to be performed."""
        await self.eventLoop.create_task(self.__startRequest())

    async def __startRequest(self) -> None:
        # Start the post or get request
        async with aiohttp.ClientSession() as session:
            if self.startingMessage == "":
                async with session.get(self.url, ssl = False) as response:
                    msg = await response.text()
                    self.receiveCallback(msg)
            else:
                async with session.post(self.url, data = self.startingMessage, 
                ssl = False) as response:
                    msg = await response.text()
                    self.receiveCallback(msg)

class EasyAsyncWS(EasyAsyncBase):
    """Handle a single websocket connection using coroutines to send/receive.
    To send a message, call .sendingQ.put_nowait method.

    receiveCallback function will have a message received parsed in as the only
    parameter.
    
    eventLoop param should be the event loop where start() and close() will be
    called from. If left as none, eventLoop will be asyncio.get_event_loop()"""
    def __init__(self, url: str, receiveCallback: Callable[[Any], None], 
        eventLoop: AbstractEventLoop = None, startingMessage: str = "", 
        closeTimeout = None
    ) -> None:
        super().__init__(
            url = url,
            receiveCallback = receiveCallback,
            eventLoop = eventLoop,
            startingMessage = startingMessage
        )
        # Create sending queue
        self.sendingQ = asyncio.Queue(loop = self.eventLoop)
        if self.startingMessage != "":
            self.sendingQ.put_nowait(startingMessage)
        self.closeTimeout = closeTimeout

    async def start(self):
        """Starts up the connection and submits the necessary coroutines for operation
        
        Reminder: Ensure the event loop its operating on never gets blocked for long.
        (E.g sleep(), input() etc). Otherwise send/receive of the socket will also halt."""
        routine = self.eventLoop.create_task(self.__startupRoutine())
        self.tasks.append(routine)
        await routine
    
    async def __startupRoutine(self): 
        # Open/close connection
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.url) as websocket:
                # Place __receiveRoutine and __sendRoutine onto the event loop
                await asyncio.gather(self.__receiveRoutine(websocket), self.__sendRoutine(websocket), loop=self.eventLoop)
    async def __receiveRoutine(self, websocket: aiohttp.ClientWebSocketResponse):
        async for msg in websocket:
            if websocket.closed == True:
                # Break coroutine on socket close
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
                # Close socket and break coroutine
                await websocket.close()
                break
            await websocket.send_str(msg)

    async def close(self):
        """Asynchronously close connection and close all coroutines associated
        with this object"""
        # Doing self.sendingQ.put_nowait("EXIT") could be simpler here?
        self.tasks.append(asyncio.create_task(self.sendingQ.put("EXIT")))
        await asyncio.gather(*self.tasks)

class ConnectionManager:
    """Manages the creation/deletion of EasyAsyncWS or EasyAsyncHTTP in
    a synchronous context.

    All connections execute on the same async event loop running
    on a seperate thread."""
    def __init__(self) -> None:
        self.threadPool = concurrent.futures.ThreadPoolExecutor()
        self.connections: Dict[str, EasyAsyncBase] = {}
        self.eventLoop = asyncio.new_event_loop()
        # Set loop running on seperate thread
        def runLoop():
            asyncio.set_event_loop(self.eventLoop)
            self.eventLoop.run_forever()
        self.eventLoopExecution = self.threadPool.submit(runLoop)
        # List to track routines to wait on before closing event loop
        self.waitingOn = []

    def addConnection(self, name: str, url: str,
        receiveCallback: Callable[[Any], None], startingMessage: str = "",
        closeTimeout = None
    ) -> None:
        """Creates a new websocket or http connection and runs it in the background.
        websocket/http connections are inferred by the protocol in the url

        If doing http connection, if startingMessage is default, then it is a get request.
        Otherwise, its a post request.

        receiveCallback function will have message received parsed in as the only
        parameter."""
        if name in self.connections:
            print("Name argument is already in use by another connection, skipping")
            return
        
        protocol = url.split("://")[0]
        # Create a ws connection
        if protocol == "ws" or protocol == "wss":
            self.connections[name] = EasyAsyncWS(
                url = url,
                receiveCallback = receiveCallback,
                startingMessage = startingMessage,
                eventLoop = self.eventLoop,
                closeTimeout = closeTimeout)
            # Submit connection to event loop
            self.waitingOn.append(
                asyncio.run_coroutine_threadsafe(self.connections[name].start(), self.eventLoop)
            )
        # Create a http connection
        elif protocol == "http" or protocol == "https": 
            self.waitingOn.append(
                asyncio.run_coroutine_threadsafe(
                    EasyAsyncHTTP(
                        url = url,
                        receiveCallback = receiveCallback,
                        eventLoop = self.eventLoop,
                        startingMessage = startingMessage
                    ).start(),
                    self.eventLoop
                )
            )
        # Show error on other protocols
        else:
            print(f"Protocol \"{protocol}\" not supported, skipping connection to {url}")

    def __getitem__(self, name: str) -> EasyAsyncWS:
        if type(self.connections[name]) == EasyAsyncWS:
            return self.connections[name]

    def closeConnection(self, name: str) -> None:
        """Closes websocket connection of a given name."""
        if name in self.connections:
            asyncio.run_coroutine_threadsafe(self.connections[name].close(), self.eventLoop)
    def close(self) -> None:
        """Waits for http requests to perform and for all websocket connections to close.
        The background thread is then shutdown."""
        for connection in self.connections.values():
            # Initiate closing if connection is a websocket
            if type(connection) == EasyAsyncWS:
                # Note: line below creates a concurrent.Future object, not asyncio.Future
                asyncio.run_coroutine_threadsafe(connection.close(), self.eventLoop)
        # Block until all routines in waiting list have finished
        # Note: waitingOn will only contain .start() coroutines, these finish when connection is closed 
        concurrent.futures.wait(self.waitingOn, return_when=concurrent.futures.ALL_COMPLETED)
        # Close the event loop and thread then block until its done
        self.eventLoop.call_soon_threadsafe(self.eventLoop.stop)
        self.eventLoopExecution.result()
 
# %%
