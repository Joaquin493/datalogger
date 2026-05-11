import os
import re
import shutil
import pandas as pd

# Path activo (único) que lee el logger. La idea es que la UI pueda
# reemplazarlo subiendo un nuevo xlsx — los anteriores se guardan en
# `xlsx_backups/` con timestamp para permitir rollback.
ACTIVE_XLSX = "tags_active.xlsx"
BACKUPS_DIR = "xlsx_backups"

# Compat: si todavía no hicimos la migración a `tags_active.xlsx` y el
# archivo original está en disco, lo renombramos in-place una vez.
_LEGACY_XLSX = "Programa_TTA_IRSA_convertido v4.xlsx"

XLSX_SHEET = "Sheet2"

_FLAG_RE = re.compile(r"%M(\d+)\.(\d+)")
_PHYS_RE = re.compile(r"%([IQ])(\d+)\.(\d+)")


def _ensure_active_xlsx():
    """Si no existe `tags_active.xlsx` pero sí el legacy, migrar (una vez)."""
    if os.path.exists(ACTIVE_XLSX):
        return
    if os.path.exists(_LEGACY_XLSX):
        shutil.copy2(_LEGACY_XLSX, ACTIVE_XLSX)


def _sort_key(t):
    """INPUT primero, luego OUTPUT; dentro de cada grupo por word/bit del %MW espejo."""
    type_rank = 0 if t["type"] == "INPUT" else 1
    return (type_rank, t["mw_word"], t["mw_bit"])


def load_tags(xlsx_path=None, overrides=None):
    """
    Carga el mapeo del PLC M221 desde la planilla activa.

    Cada fila de Sheet2 vincula:
      - Address  → dirección física %I/%Q (informativa, para mostrar/loguear)
      - Flag HR  → bit dentro de un %MW (formato "%M<word>.<bit>") al que el
                   programa del PLC copia el estado físico. Es lo que se lee
                   por Modbus FC03, ya que el M221 no expone %I/%Q vía Modbus.
      - TYPE     → INPUT u OUTPUT

    Si `overrides` es un dict {address: {symbol?, description?, type?}},
    los valores del dict pisan a los del xlsx por address. Esto permite
    corregir descripciones/símbolos sin modificar el archivo original.
    """
    if xlsx_path is None:
        _ensure_active_xlsx()
        xlsx_path = ACTIVE_XLSX

    df = pd.read_excel(xlsx_path, sheet_name=XLSX_SHEET)
    overrides = overrides or {}
    tags = []

    for _, row in df.iterrows():
        address = str(row.get("Address", "nan")).strip()
        flag    = str(row.get("Flag HR", "nan")).strip()
        symbol  = str(row.get("Symbol", "nan")).strip()
        comment = str(row.get("Comment", "nan")).strip()
        typ     = str(row.get("TYPE", "nan")).strip().upper()

        if not _PHYS_RE.match(address):
            continue
        m = _FLAG_RE.match(flag)
        if not m:
            continue
        if typ not in ("INPUT", "OUTPUT"):
            continue

        word, bit = int(m.group(1)), int(m.group(2))

        if symbol in ("nan", ""):
            symbol = address
        if comment == "nan":
            comment = ""

        # Aplicar override si existe para este address.
        ov = overrides.get(address)
        overridden = False
        if ov:
            if ov.get("symbol"):
                symbol = ov["symbol"]
                overridden = True
            if ov.get("description") is not None:
                comment = ov["description"]
                overridden = True
            if ov.get("signal_type") in ("INPUT", "OUTPUT"):
                typ = ov["signal_type"]
                overridden = True

        tags.append({
            "address":     address,
            "tag":         symbol,
            "description": comment,
            "type":        typ,
            "mw_word":     word,
            "mw_bit":      bit,
            "overridden":  overridden,
        })

    tags.sort(key=_sort_key)
    return tags


def validate_xlsx(path):
    """Levanta ValueError si el xlsx no tiene la estructura esperada.

    Usado antes de aceptar un upload: si la nueva planilla está rota, no
    pisamos la activa.
    """
    try:
        df = pd.read_excel(path, sheet_name=XLSX_SHEET)
    except Exception as e:
        raise ValueError(f"No se pudo leer la hoja '{XLSX_SHEET}': {e}")

    required = {"Address", "Flag HR", "TYPE"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas: {sorted(missing)}")

    n_valid = 0
    for _, row in df.iterrows():
        address = str(row.get("Address", "nan")).strip()
        flag    = str(row.get("Flag HR", "nan")).strip()
        typ     = str(row.get("TYPE", "nan")).strip().upper()
        if (_PHYS_RE.match(address) and _FLAG_RE.match(flag)
                and typ in ("INPUT", "OUTPUT")):
            n_valid += 1
    if n_valid == 0:
        raise ValueError("La planilla no contiene ninguna fila válida (Address %I/%Q + Flag HR + TYPE).")
    return n_valid
