from PyEasySocket import ConnectionManager
from queue import Queue
import time

connections = ConnectionManager()
tests = {
    "ws1": ["wss://stream.binance.com:9443/ws/ethusdt@depth10@100ms", Queue()],
    "ws2": ["wss://stream.binance.com:9443/ws/btcusdt@depth@100ms", Queue()],
    "get": ["https://python.org", Queue()],
    "post": ["https://httpbin.org/post", Queue()]

}

# Start ws connections
connections.addConnection(
    name = "ws1",
    url = tests["ws1"][0],
    receiveCallback = lambda msg: tests["ws1"][1].put(msg)
)
connections.addConnection(
    name = "ws2",
    url = tests["ws2"][0],
    receiveCallback = lambda msg: tests["ws2"][1].put(msg)
)
# Wait for ws connections to receive some data
time.sleep(3)
# Start http requests
connections.addConnection("get", tests["get"][0], lambda msg: tests["get"][1].put(msg))
connections.addConnection("testing", tests["post"][0], lambda msg: tests["post"][1].put(msg), b'Hello world!')
# Close all connections as soon as possible
connections.close()

for test in tests:
    print(test, tests[test][1].qsize())

