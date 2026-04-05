import random
import time
import threading

from pymodbus.server import StartTcpServer
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusDeviceContext, ModbusServerContext

TOTAL_SIGNALS = 200

block = ModbusSequentialDataBlock(0, [0] * (TOTAL_SIGNALS + 8))
store = ModbusDeviceContext(co=block)
context = ModbusServerContext(devices=store, single=True)

def random_changes():
    while True:
        address = random.randint(0, TOTAL_SIGNALS - 1)
        current = context[0].getValues(1, address, count=1)[0]
        new = 0 if current else 1
        context[0].setValues(1, address, [new])
        print("SIM SIGNAL", address, "=", new)
        time.sleep(random.uniform(1, 3))

def start_simulator():
    threading.Thread(target=random_changes, daemon=True).start()
    print("MODBUS SIMULATOR RUNNING ON PORT 502")
    StartTcpServer(context=context, address=("0.0.0.0", 5020))

if __name__ == "__main__":
    start_simulator()