"""Simulador Modbus TCP para testing local.

Imita el mapeo del PLC M221 que el `modbus_logger` espera:
  - %MW0..%MW50    : registros analógicos (bloque 1)
  - %MW100..%MW114 : registros analógicos (bloque 2)
  - %MW200..%MW203 : espejo de inputs físicos (cada bit = un %I)
  - %MW210..%MW212 : espejo de outputs físicos (cada bit = un %Q)

El M221 NO expone %I/%Q vía Modbus, todo se hace contra Holding Registers
(FC03). Este simulador refleja esa misma restricción.

Para usar localmente:
  1) Correr este archivo: python modbus_simulator.py
     (escucha en 0.0.0.0:5020 con device_id=1)
  2) Apuntar el logger al simulador:
     PowerShell:  $env:PLC_IP="127.0.0.1"; $env:PLC_PORT="5020"; python main.py
     Bash:        PLC_IP=127.0.0.1 PLC_PORT=5020 python main.py
"""
from __future__ import annotations

import os
import random
import threading
import time

from pymodbus.server import StartTcpServer
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusDeviceContext,
    ModbusServerContext,
)

# ---------------------------------------------------------------------------
# CONFIGURACIÓN — debe matchear modbus_logger.py
# ---------------------------------------------------------------------------

SIM_HOST    = os.environ.get("SIM_HOST", "0.0.0.0")
SIM_PORT    = int(os.environ.get("SIM_PORT", "5020"))
TICK_S      = float(os.environ.get("SIM_TICK_S", "1.5"))   # segundos entre cambios

# Burst periódico — cada N ticks, en vez de un solo cambio, dispara M bit
# toggles seguidos. Sirve para simular eventos correlacionados (ej. una
# secuencia de arranque que activa muchos relés a la vez) y dejar ver al
# logger procesar lotes grandes en un solo ciclo de poll.
BURST_EVERY = int(os.environ.get("SIM_BURST_EVERY", "20"))  # 0 = desactivado
BURST_SIZE  = int(os.environ.get("SIM_BURST_SIZE",  "50"))

# Mapeo I/O bits dentro de los HRs (debe coincidir con la xlsx del PLC).
# (word_addr, bits_usados) — bits_usados es la cantidad de bits "vivos" en esa word.
INPUT_WORDS  = [(200, 16), (201, 8), (202, 16), (203, 16)]   # 56 bits totales
OUTPUT_WORDS = [(210, 16), (211, 16), (212, 16)]              # 48 bits totales

# Bloques analógicos (registros numéricos del programa).
ANALOG_BLOCKS = [(0, 51), (100, 20)]

# Tamaño total del datastore HR — cubre desde 0 hasta el último word usado + holgura.
HR_SIZE = 256


# ---------------------------------------------------------------------------
# DATASTORE — un único bloque HR de tamaño fijo cubriendo todos los rangos.
# ---------------------------------------------------------------------------

hr_block = ModbusSequentialDataBlock(0, [0] * HR_SIZE)
store = ModbusDeviceContext(hr=hr_block)
context = ModbusServerContext(devices=store, single=True)


def _read_word(addr: int) -> int:
    return context[0].getValues(3, addr, count=1)[0]   # FC=3 (HR)


def _write_word(addr: int, value: int) -> None:
    context[0].setValues(3, addr, [value & 0xFFFF])


def _toggle_bit(word_addr: int, bit: int) -> bool:
    """Invierte un bit dentro de un HR. Devuelve el nuevo valor del bit."""
    cur = _read_word(word_addr)
    mask = 1 << bit
    new = cur ^ mask
    _write_word(word_addr, new)
    return bool(new & mask)


# ---------------------------------------------------------------------------
# Generador de cambios — simula actividad del PLC
# ---------------------------------------------------------------------------

def _toggle_random_io_bit(quiet: bool = False) -> None:
    """Toggle aleatorio de un bit I/O (input o output). Si `quiet`, no imprime."""
    if random.random() < 0.5:
        word, n_bits = random.choice(INPUT_WORDS)
        kind = "INPUT "
    else:
        word, n_bits = random.choice(OUTPUT_WORDS)
        kind = "OUTPUT"
    bit = random.randrange(n_bits)
    new = _toggle_bit(word, bit)
    if not quiet:
        print(f"SIM  {kind} %MW{word}.bit{bit:<2}  -> {'1' if new else '0'}")


def _do_burst() -> None:
    """Aplica BURST_SIZE toggles de bits I/O en rapida sucesion. Como el logger
    poolea cada 500ms, todos quedan agrupados en el mismo ciclo y se guardan
    como un lote de eventos correlacionados."""
    print(f"SIM  ====== BURST de {BURST_SIZE} cambios ======")
    for _ in range(BURST_SIZE):
        _toggle_random_io_bit(quiet=True)
    print(f"SIM  ====== fin del burst ======")


def random_changes():
    """Cada TICK_S elige aleatoriamente: toggle de bit I/O, o cambio de analógico.
    Cada BURST_EVERY ticks, en lugar del cambio simple dispara un burst grande."""
    print(f"SIM tick={TICK_S}s — generando cambios aleatorios"
          + (f" + burst de {BURST_SIZE} cada {BURST_EVERY} ticks" if BURST_EVERY else ""))
    tick = 0
    while True:
        tick += 1
        # ¿Toca burst en este tick?
        if BURST_EVERY and tick % BURST_EVERY == 0:
            _do_burst()
        else:
            roll = random.random()
            if roll < 0.85:
                _toggle_random_io_bit()
            else:
                # Analógico — valor nuevo en un %MW al azar (no se logea como
                # evento porque el logger ya no lee los bloques analogicos,
                # pero sirve para que el datastore tenga datos vivos).
                base, count = random.choice(ANALOG_BLOCKS)
                offset = random.randrange(count)
                value  = random.randint(0, 65535)
                _write_word(base + offset, value)
                print(f"SIM  ANALOG %MW{base + offset:<4}      -> {value}")
        time.sleep(TICK_S)


def start_simulator():
    threading.Thread(target=random_changes, daemon=True).start()
    print("=" * 60)
    print(f"MODBUS SIMULATOR — escuchando en {SIM_HOST}:{SIM_PORT}")
    print(f"  Inputs (bits)   : %MW200..%MW203  ({sum(b for _, b in INPUT_WORDS)} bits)")
    print(f"  Outputs (bits)  : %MW210..%MW212  ({sum(b for _, b in OUTPUT_WORDS)} bits)")
    print(f"  Analog (%MW)    : %MW0..%MW50, %MW100..%MW114")
    print(f"  Tick            : {TICK_S}s")
    if BURST_EVERY:
        print(f"  Burst           : {BURST_SIZE} cambios cada {BURST_EVERY} ticks (~{BURST_EVERY * TICK_S:.0f}s)")
    else:
        print(f"  Burst           : desactivado")
    print(f"  Device ID       : 1 (single=True)")
    print(f"Apuntar el logger:  PLC_IP={SIM_HOST if SIM_HOST != '0.0.0.0' else '127.0.0.1'} PLC_PORT={SIM_PORT}")
    print("=" * 60)
    StartTcpServer(context=context, address=(SIM_HOST, SIM_PORT))


if __name__ == "__main__":
    start_simulator()
