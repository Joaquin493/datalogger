from pymodbus.client import ModbusTcpClient

client = ModbusTcpClient("192.168.68.84", port=502)
client.connect()

# Prender y apagar algunos coils para generar eventos
import time

for i in range(5):
    client.write_coil(i, True, device_id=1)
    print(f"Coil {i} → ON")
    time.sleep(1)
    client.write_coil(i, False, device_id=1)
    print(f"Coil {i} → OFF")
    time.sleep(1)

client.close()
print("Listo")