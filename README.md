# ⚡ Datalogger

Sistema de registro de eventos para variables digitales vía Modbus TCP, con interfaz web accesible desde cualquier terminal de la red LAN.

---

## ¿Qué hace?

- Lee hasta **200 señales digitales** desde un PLC via Modbus TCP cada 500ms
- Registra cada cambio de estado con estampa de tiempo en una base de datos local
- Muestra el estado en tiempo real desde cualquier navegador de la red
- Exporta el historial de eventos a Excel

---

## Estructura del proyecto

```
datalogger/
├── static/
│   ├── app.js          # Lógica del frontend
│   └── style.css       # Estilos
├── templates/
│   └── index.html      # Interfaz web
├── main.py             # Servidor web FastAPI
├── modbus_logger.py    # Lectura Modbus y escritura en DB
├── tag_loader.py       # Carga de tags desde Excel
├── modbus_simulator.py # Simulador PLC para pruebas
└── Programa_TTA_IRSA_convertido.xlsx  # Listado de tags
```

---

## Requisitos

**Python 3.10 o superior**

Instalar dependencias:

```bash
pip install fastapi uvicorn pymodbus pandas openpyxl jinja2
```

---

## Configuración

En `modbus_logger.py` configurar la IP y puerto del PLC:

```python
PLC_IP   = "192.168.54.10"  # IP del PLC
PLC_PORT = 502               # Puerto Modbus TCP
```

El archivo Excel de tags debe tener estas columnas:

| Columna    | Ejemplo              |
|------------|----------------------|
| Dirección  | %I0.0                |
| Símbolo    | P_EMERG              |
| Comentario | PARADA DE EMERGENCIA |

---

## Ejecución

### Con PLC real

```bash
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Acceder desde el navegador:
```
http://<IP-DE-ESTA-PC>:8000
```

### Sin PLC — modo simulación

Abrir dos terminales:

**Terminal 1:**
```bash
python modbus_simulator.py
```

**Terminal 2:**
```bash
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Asegurarse de que en `modbus_logger.py` la IP sea `127.0.0.1` para usar el simulador.

---

## Interfaz web

| Pestaña | Descripción |
|---|---|
| Panel de Señales | Estado actual de todas las señales en tiempo real |
| Registro de Eventos | Historial con filtros por tag, estado, fecha y descripción |
| Contadores | Estadísticas de actividad por tag |

Desde el header se puede exportar todos los eventos a un archivo `.xlsx`.

---

## Base de datos

Los eventos se almacenan en `events.db` (SQLite). El sistema aplica un comportamiento **FIFO** con un límite de 100.000 registros — cuando se supera ese límite, los eventos más antiguos se eliminan automáticamente.

---

## Protocolo

- **Modbus TCP** — función 01 (Read Coils)
- Puerto por defecto: **502**
- Compatible con PLCs Schneider y otros que soporten Modbus TCP estándar