from EasySocket import ConnectionManager
from queue import Queue
import time

connections = ConnectionManager()
msgs = Queue()

connections.addConnection(
    name = "binance1",
    url = "wss://stream.binance.com:9443/ws/ethusdt@depth10@100ms",
    receiveCallback = lambda msg: msgs.put(msg)
)
connections.addConnection(
    name = "binance2",
    url = "wss://stream.binance.com:9443/ws/btcusdt@depth10@100ms",
    receiveCallback = lambda msg: msgs.put(msg)
)

time.sleep(5)

while not msgs.empty():
    msg = msgs.get()
    print(msg)

connections.close()