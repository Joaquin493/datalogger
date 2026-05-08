# ⚡ Datalogger

Sistema de registro de eventos para variables digitales vía Modbus TCP, con interfaz web accesible desde cualquier terminal de la red LAN.

---

## ¿Qué hace?

- Lee 56 entradas digitales + 48 salidas digitales + 71 registros analógicos desde un PLC Schneider M221 via Modbus TCP cada 500 ms.
- Registra cada cambio de estado con estampa de tiempo en una base de datos local (SQLite).
- Muestra el estado en tiempo real desde cualquier navegador de la red (auto-refresh, tema claro/oscuro, atajos de teclado).
- Exporta el historial de eventos a Excel o CSV streaming.

> **Nota M221:** este controlador no expone %I/%Q vía Modbus. El programa del PLC copia las I/O físicas a bits de %MW (`%MW200..%MW203` para inputs y `%MW210..%MW212` para outputs), y el logger las lee como Holding Registers (FC03). Ver `Programa_TTA_IRSA_convertido v4.xlsx` para el mapeo completo.

---

## Estructura del proyecto

```
datalogger/
├── static/
│   ├── app.js          # SPA frontend (vanilla JS)
│   └── style.css       # Estilos (tema claro/oscuro)
├── templates/
│   ├── index.html      # Dashboard
│   └── login.html      # Login
├── main.py             # Servidor web FastAPI + endpoints /api/*
├── modbus_logger.py    # Polling Modbus FC03 + escritura en DB
├── tag_loader.py       # Carga de mapeo desde xlsx
├── modbus_simulator.py # Simulador PLC para pruebas locales
└── Programa_TTA_IRSA_convertido v4.xlsx   # Mapeo I/O físico ↔ %MW
```

---

## Requisitos

- **Python 3.10 o superior**
- Conectividad de red al PLC (puerto 502 TCP)
- Navegador moderno en los clientes (Chrome, Edge, Firefox actualizados)

```powershell
pip install -r requirements.txt
```

---

## Configuración (variables de entorno)

| Variable | Default | Descripción |
|---|---|---|
| `PLC_IP` | `192.168.200.10` | IP del PLC M221 |
| `PLC_PORT` | `502` | Puerto Modbus TCP |
| `APP_USER` | `admin` | Usuario para login web |
| `APP_PASSWORD` | `admin` | Contraseña del usuario |

En `modbus_logger.py` también se puede ajustar:
- `DEVICE_ID` (línea 60) — `1` por default. Si en EcoStruxure Machine Expert Basic no está habilitado "Modbus Mapping" del M221, cambiar a `255`.

---

## Probar localmente con simulador

Útil para validar la web sin tener el PLC delante.

**Terminal 1 — simulador:**
```powershell
python modbus_simulator.py
```

**Terminal 2 — app:**
```powershell
$env:PLC_IP="127.0.0.1"
$env:PLC_PORT="5020"
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Abrir http://localhost:8000 (login `admin` / `admin`).

---

## 🚀 Puesta en marcha en producción

### 1. Pre-requisitos en la PC del datalogger

- Windows 10/11 (o Linux) con Python 3.10+ instalado.
- IP fija en la red del PLC y firewall que permita el puerto **8000** entrante (para que los clientes accedan).
- Acceso de red al PLC en puerto **502**. Verificar con: `Test-NetConnection 192.168.200.10 -Port 502` (PowerShell).

### 2. Bajar el repo

```powershell
git clone https://github.com/Joaquin493/datalogger.git
cd datalogger
pip install -r requirements.txt
```

### 3. Verificar el mapeo del PLC

Abrir `Programa_TTA_IRSA_convertido v4.xlsx`, hoja `Sheet2`. Cada fila debe tener:
- `Address` con la I/O física (`%I0.0`, `%Q3.5`, etc.)
- `Flag HR` con el bit espejado (`%M200.0`, `%M211.5`, etc.)
- `TYPE` = `INPUT` o `OUTPUT`
- `Symbol` (tag) y `Comment`

> Si cambia el mapeo en el PLC, **actualizar la xlsx y reiniciar la app** — los rangos de %MW que el logger lee se calculan al arrancar a partir del archivo.

### 4. Configurar credenciales y conexión

Crear un archivo `start.ps1` en la carpeta del proyecto:

```powershell
# start.ps1
$env:PLC_IP       = "192.168.200.10"
$env:PLC_PORT     = "502"
$env:APP_USER     = "operador"
$env:APP_PASSWORD = "una-clave-fuerte-aca"

python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

> ⚠️ **No commitear `start.ps1` con la contraseña.** El `.gitignore` ya excluye `.env`; si querés versionar el script de arranque, sacá las credenciales y leelas de un archivo aparte o de Credentials Manager de Windows.

### 5. Primera corrida manual (smoke test)

```powershell
.\start.ps1
```

En la consola debería aparecer:

```
LOGGER INICIADO
  PLC:              192.168.200.10:502
  Device ID:        1
  Inputs:           56  (espejo en %MW200..%MW203)
  Outputs:          48  (espejo en %MW210..%MW212)
  ...
Conexión establecida con 192.168.200.10:502
```

Si en cambio aparece `Lectura error: Modbus Error: [Input/Output] No Response received from the remote unit`, revisar:

1. Que el PLC esté energizado y respondiendo: `Test-NetConnection 192.168.200.10 -Port 502`
2. Que el `DEVICE_ID` coincida con el del M221 (default 255 si no hay Modbus Mapping habilitado).
3. Que `PLC_IP` apunte a la IP correcta.

Verificar desde otra PC de la red: `http://<IP-de-esta-PC>:8000`. Login con las credenciales del paso 4.

### 6. Configurar autoarranque al boot (Windows — Tarea Programada)

Para que la app se levante sola después de un reinicio:

```powershell
# Como administrador
$action  = New-ScheduledTaskAction -Execute "powershell.exe" `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File C:\Users\joaqu\Documents\datalogger\start.ps1" `
  -WorkingDirectory "C:\Users\joaqu\Documents\datalogger"

$trigger  = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
  -StartWhenAvailable -AllowStartIfOnBatteries

Register-ScheduledTask -TaskName "Datalogger" `
  -Action $action -Trigger $trigger -Principal $principal -Settings $settings
```

Probar el reinicio: `shutdown /r /t 0`. Después del boot, `http://<IP>:8000` debería responder en ~30 s.

> **Linux / IOT2050 / Raspberry Pi:** usar `systemd`. Crear `/etc/systemd/system/datalogger.service`:
> ```ini
> [Unit]
> Description=Datalogger Modbus M221
> After=network-online.target
>
> [Service]
> Type=simple
> WorkingDirectory=/opt/datalogger
> EnvironmentFile=/opt/datalogger/.env
> ExecStart=/usr/bin/python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
> Restart=on-failure
> RestartSec=5
>
> [Install]
> WantedBy=multi-user.target
> ```
> `sudo systemctl enable --now datalogger`

### 7. Verificar acceso de los clientes

Desde una PC operadora en la misma LAN: `http://<IP-del-server>:8000`.

- Health check sin login: `http://<IP>:8000/healthz` debería devolver `{"status":"ok",...}` cuando el PLC está conectado, o `degraded` si no.
- En el header del dashboard, el indicador `● PLC CONECTADO · XXms` confirma que el ciclo Modbus está corriendo (XX = latencia del último ciclo).

### 8. Backup de la base de datos

`events.db` es la única fuente de los eventos. Hacer copias periódicas. Como SQLite está en modo WAL, la forma segura es:

```powershell
# Backup en caliente (no requiere parar la app)
python -c "import sqlite3; src=sqlite3.connect('events.db'); dst=sqlite3.connect('events_backup.db'); src.backup(dst); src.close(); dst.close()"
```

Tarea programada diaria con esto + copia a un network share / OneDrive.

### 9. Logs

Logs rotativos en `logs/logger.log` (5 archivos × 1 MB). Para verlos en vivo:

```powershell
Get-Content logs/logger.log -Wait -Tail 50
```

`logs/` está en `.gitignore` — no se versiona.

---

## Mantenimiento

| Tarea | Cómo |
|---|---|
| Cambiar mapeo I/O | Actualizar xlsx → reiniciar la app |
| Cambiar credenciales | Editar `start.ps1` → reiniciar la tarea programada |
| Ver eventos viejos | El sistema aplica FIFO con tope de 1.000.000 registros (~200-300 MB). Cuando se supera, borra los más antiguos. Para conservar más, hacer backups periódicos. |
| Ver latencia del PLC | `http://<IP>:8000/healthz` o el header del dashboard |
| Reiniciar app | `Restart-ScheduledTask -TaskName Datalogger` |

---

## Interfaz web

| Pestaña | Descripción |
|---|---|
| **Panel de Señales** | Estado actual de las 104 señales (animación al cambiar) |
| **Registro de Eventos** | Historial con filtros, ordenamiento, paginado y auto-refresh |
| **Contadores** | Total / ON / OFF / último evento por tag |
| **Sistema** | Conexiones, desconexiones y arranques |

**Atajos de teclado:** `1`–`4` cambian de tab, `/` enfoca búsqueda, `Esc` limpia filtros, `T` alterna tema claro/oscuro.

**Exportar:** botones `↓ XLSX` (rápido, hasta 50k filas) y `↓ CSV` (streaming, apto para el FIFO completo).

---

## API

Todos los endpoints `/api/*` requieren cookie de sesión (login previo).

| Endpoint | Descripción |
|---|---|
| `GET /healthz` | Liveness sin auth (para monitoreo externo) |
| `GET /api/status` | Estado del enlace Modbus + total de eventos |
| `GET /api/variables` | Snapshot del último estado de cada señal |
| `GET /api/events?...` | Historial filtrado, paginado, ordenado |
| `GET /api/stats` | Agregados por tag |
| `GET /api/sysevents` | Eventos del sistema |
| `GET /api/export.xlsx` / `.csv` | Export con filtros aplicados |

---

## Protocolo

- **Modbus TCP** — Función 03 (Read Holding Registers) sobre `%MW0–%MW50`, `%MW100–%MW114`, `%MW200–%MW203` (inputs espejados) y `%MW210–%MW212` (outputs espejados).
- Puerto por defecto: **502**.
- Específico para M221 (cualquier PLC que mapee I/O en HRs sirve con la xlsx adecuada).
