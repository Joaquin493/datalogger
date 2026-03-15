import time
from pymodbus.client import ModbusTcpClient
from tag_loader import load_tags

PLC_IP = "127.0.0.1"
PLC_PORT = 502
TOTAL_SIGNALS = 185

tags = load_tags()
client = ModbusTcpClient(PLC_IP, port=PLC_PORT)

if not client.connect():
    print("ERROR: No se pudo conectar al PLC en", PLC_IP)
    exit()

print("Conectado a", PLC_IP)

def read_all_coils():
    values = []
    for address in range(0, TOTAL_SIGNALS, 8):
        count = min(8, TOTAL_SIGNALS - address)
        result = client.read_coils(address=address, count=count, device_id=1)
        if not hasattr(result, 'bits'):
            raise Exception(f"Error en address {address}: {result}")
        values.extend(result.bits[:count])
    return values

try:
    while True:
        values = read_all_coils()
        print("\033c", end="")
        print(f"{'ADDR':<10} {'TAG':<20} {'DESCRIPCION':<40} {'ESTADO'}")
        print("-" * 80)
        for i, val in enumerate(values):
            addr  = tags[i]["address"]      if i < len(tags) else f"%I{i//16}.{i%16}"
            tag   = tags[i]["tag"]          if i < len(tags) else f"TAG_{i}"
            desc  = tags[i]["description"]  if i < len(tags) else ""
            estado = "ON  ✓" if val else "OFF"
            print(f"{addr:<10} {tag:<20} {desc:<40} {estado}")
        print("\nActualizando cada 1s... Ctrl+C para salir")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nDesconectado.")
    client.close()