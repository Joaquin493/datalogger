# ⚡ Datalogger

Sistema de registro de eventos para variables digitales vía Modbus TCP, con interfaz web accesible desde cualquier terminal de la red LAN.

---

## ¿Qué hace?

- Lee las entradas y salidas digitales de un PLC Schneider M221 vía Modbus TCP cada 500 ms.
- Registra cada cambio de estado con estampa de tiempo en una base de datos local (SQLite, FIFO de 1.000.000 eventos).
- Muestra el estado en tiempo real desde cualquier navegador de la red (auto-refresh, tema claro/oscuro, atajos de teclado).
- Exporta el historial de eventos a Excel o CSV streaming.
- Permite editar el mapeo (symbol/descripción/tipo) y reemplazar el xlsx desde la propia interfaz, con previsualización del diff antes de aplicar y rollback a versiones anteriores. Los cambios se recargan en caliente sin reiniciar el servicio.

> **Nota M221:** este controlador no expone %I/%Q vía Modbus. El programa del PLC copia cada bit físico a un bit de %MW (la columna `Flag HR` del xlsx), y el logger los lee como Holding Registers (FC03). El rango exacto de %MW espejados se calcula al arrancar a partir del archivo `tags_active.xlsx`.

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
├── tag_loader.py       # Carga de mapeo desde xlsx + overrides
├── modbus_simulator.py # Simulador PLC para pruebas locales
├── Programa_TTA_IRSA_convertido v4.xlsx   # Semilla del mapeo (versionada)
├── tags_active.xlsx    # Mapeo en uso (generado en runtime, no se versiona)
└── xlsx_backups/       # Backups automáticos antes de cada upload/rollback
```

> En el primer arranque, si `tags_active.xlsx` no existe, se copia desde la semilla. A partir de ahí, las correcciones desde la UI se guardan en la tabla `tag_overrides` (DB), y los uploads/rollbacks operan sobre `tags_active.xlsx`. La semilla solo se vuelve a usar si se borra el activo.

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
| `PLC_IP` | `10.10.145.244` | IP del PLC M221 (default = la red de prod) |
| `PLC_PORT` | `502` | Puerto Modbus TCP |
| `APP_USER` | `admin` | Usuario para login web |
| `APP_PASSWORD` | `admin` | Contraseña del usuario |

> Los defaults ya apuntan al PLC real, así que en prod no hace falta setear env vars para arrancar — el único motivo para tocarlas es cambiar las credenciales de login.

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
- Acceso de red al PLC en puerto **502**. Verificar con: `Test-NetConnection 10.10.145.244 -Port 502` (PowerShell).

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

> Si cambia el mapeo en el PLC, regenerar la planilla desde EcoStruxure y subirla desde la pestaña **Sistema → Configuración de tags → ⬆ Subir nuevo xlsx**. La UI muestra un preview con el diff (agregados / eliminados / modificados / overrides huérfanos) antes de aplicar, y el archivo anterior queda respaldado automáticamente. La recarga es en caliente — no hace falta reiniciar el servicio.

### 4. (Opcional) Cambiar credenciales de login

La IP del PLC y el puerto ya están hardcodeados en el código apuntando a prod, así que **no hace falta ningún script de arranque ni archivo `.env`**. Solo si querés cambiar el usuario/contraseña del login web, seteá las env vars de forma persistente con `setx` (una sola vez):

```powershell
setx APP_USER "operador"
setx APP_PASSWORD "una-clave-fuerte-aca"
```

> Los valores persisten para sesiones futuras del usuario. Las env vars **no se commitean** porque no están en archivos del repo.

### 5. Primera corrida manual (smoke test)

```powershell
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

En la consola debería aparecer (cantidades exactas según `tags_active.xlsx`):

```
LOGGER INICIADO
  PLC:              10.10.145.244:502
  Device ID:        1
  Inputs:           N  (espejo en %MW<base>..%MW<base+count-1>)
  Outputs:          M  (espejo en %MW<base>..%MW<base+count-1>)
  Total signals:    N+M
  ...
Conexión establecida con 10.10.145.244:502
Estado inicial sembrado (N+M signals) — los próximos ciclos detectan cambios
```

Si en cambio aparece `Lectura error: Modbus Error: [Input/Output] No Response received from the remote unit`, revisar:

1. Que el PLC esté energizado y respondiendo: `Test-NetConnection 10.10.145.244 -Port 502`
2. Que el `DEVICE_ID` coincida con el del M221 (default 255 si no hay Modbus Mapping habilitado).
3. Que la PC tenga ruta a la red del PLC.

Verificar desde otra PC de la red: `http://<IP-de-esta-PC>:8000`. Login con las credenciales (default `admin`/`admin` si no las cambiaste con `setx`).

### 6. Configurar autoarranque al boot (Windows — Tarea Programada)

Para que la app se levante sola después de un reinicio:

```powershell
# Como administrador
$action  = New-ScheduledTaskAction -Execute "python.exe" `
  -Argument "-m uvicorn main:app --host 0.0.0.0 --port 8000" `
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
| Corregir symbol/descripción/tipo de un tag | Pestaña **Sistema** → fila → **Editar**. Se guarda como override en la DB, no toca el xlsx. Recarga en caliente. |
| Agregar tags nuevos (cambió el programa del PLC) | Regenerar xlsx desde EcoStruxure → **Sistema → ⬆ Subir nuevo xlsx**. Mostrar preview → confirmar. |
| Volver a una versión anterior del xlsx | **Sistema → Backups…** → Restaurar. El activo se respalda primero. |
| Cambiar credenciales | `setx APP_USER ...` + `setx APP_PASSWORD ...` → reiniciar la tarea programada |
| Ver eventos viejos | El sistema aplica FIFO con tope de 1.000.000 registros (~200-300 MB). Cuando se supera, borra los más antiguos. Hacer backups periódicos para conservar más historia. |
| Ver latencia del PLC | `http://<IP>:8000/healthz` o el header del dashboard |
| Reiniciar app | `Restart-ScheduledTask -TaskName Datalogger` |

---

## Interfaz web

| Pestaña | Descripción |
|---|---|
| **Panel de Señales** | Estado actual de cada I/O (animación al cambiar, filtros ON/OFF, búsqueda) |
| **Registro de Eventos** | Historial con filtros, ordenamiento, paginado y auto-refresh |
| **Contadores** | Total / ON / OFF / último evento por tag |
| **Sistema** | Configuración de tags (editar overrides, subir/descargar xlsx con preview, gestionar backups) + eventos del sistema (conexiones, desconexiones, recargas, arranques) |

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
| `GET /api/tags` | Tags efectivos (xlsx + overrides) con fechas y contadores |
| `PATCH /api/tags/{address}` | Crear/modificar un override (symbol, description, type) |
| `DELETE /api/tags/{address}/override` | Quitar override (vuelve al valor del xlsx) |
| `POST /api/tags/preview` | Subir xlsx en modo preview, devuelve diff + token de pending |
| `POST /api/tags/upload/confirm` | Aplicar un pending previamente previsualizado |
| `DELETE /api/tags/preview/{token}` | Descartar un pending |
| `GET /api/tags/backups` | Lista de backups + xlsx activo |
| `POST /api/tags/rollback` | Restaurar un backup como xlsx activo |
| `GET /api/tags/download` / `/download/{name}` | Descargar xlsx activo o un backup |

---

## Protocolo

- **Modbus TCP** — Función 03 (Read Holding Registers) sobre el rango de %MW que el PLC usa para espejar las I/O (definido en la columna `Flag HR` del xlsx; el logger calcula al arrancar los dos bloques contiguos que necesita: uno para inputs y uno para outputs).
- Puerto por defecto: **502**.
- Específico para M221 (cualquier PLC que mapee I/O a bits dentro de Holding Registers sirve con la xlsx adecuada).
