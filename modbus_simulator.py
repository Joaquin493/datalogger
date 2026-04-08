import random
import time
import threading

from pymodbus.server import StartTcpServer
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusDeviceContext,
    ModbusServerContext,
)

# Debe coincidir con modbus_logger.py
TOTAL_INPUTS  = 30   # Discrete Inputs (%I) — function code 2
TOTAL_OUTPUTS = 15   # Coils (%Q)           — function code 1
SIM_PORT      = 5020

# Datastores separados para cada tipo de registro Modbus
#   co = Coils            (FC 01 / FC 05 / FC 15)  — read_coils
#   di = Discrete Inputs  (FC 02)                   — read_discrete_inputs
#   hr = Holding Registers (FC 03 / FC 06 / FC 16)
#   ir = Input Registers   (FC 04)
coils_block  = ModbusSequentialDataBlock(0, [0] * (TOTAL_OUTPUTS + 8))
inputs_block = ModbusSequentialDataBlock(0, [0] * (TOTAL_INPUTS + 8))

store = ModbusDeviceContext(
    co=coils_block,
    di=inputs_block,
)
context = ModbusServerContext(devices=store, single=True)


def random_changes():
    """Cambia señales al azar para simular actividad del PLC."""
    while True:
        # Elegir tipo de señal al azar
        if random.random() < 0.5:
            # Discrete Input
            address = random.randint(0, TOTAL_INPUTS - 1)
            fc = 2  # discrete inputs
            current = context[0].getValues(fc, address, count=1)[0]
            new = 0 if current else 1
            context[0].setValues(fc, address, [new])
            label = f"DI (Input)  addr={address}"
        else:
            # Coil
            address = random.randint(0, TOTAL_OUTPUTS - 1)
            fc = 1  # coils
            current = context[0].getValues(fc, address, count=1)[0]
            new = 0 if current else 1
            context[0].setValues(fc, address, [new])
            label = f"CO (Output) addr={address}"

        state = "ON" if new else "OFF"
        print(f"SIM  {label}  → {state}")
        time.sleep(random.uniform(1, 3))


def start_simulator():
    threading.Thread(target=random_changes, daemon=True).start()
    print(f"MODBUS SIMULATOR RUNNING ON PORT {SIM_PORT}")
    print(f"  Discrete Inputs: {TOTAL_INPUTS}  (FC 02)")
    print(f"  Coils:           {TOTAL_OUTPUTS}  (FC 01)")
    StartTcpServer(context=context, address=("0.0.0.0", SIM_PORT))


if __name__ == "__main__":
    start_simulator()
