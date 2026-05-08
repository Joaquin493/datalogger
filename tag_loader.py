import re
import pandas as pd

XLSX_FILE  = "Programa_TTA_IRSA_convertido v4.xlsx"
XLSX_SHEET = "Sheet2"

_FLAG_RE = re.compile(r"%M(\d+)\.(\d+)")
_PHYS_RE = re.compile(r"%([IQ])(\d+)\.(\d+)")


def _sort_key(t):
    """INPUT primero, luego OUTPUT; dentro de cada grupo por word/bit del %MW espejo."""
    type_rank = 0 if t["type"] == "INPUT" else 1
    return (type_rank, t["mw_word"], t["mw_bit"])


def load_tags():
    """
    Carga el mapeo del PLC M221 desde la planilla v4.

    Cada fila de Sheet2 vincula:
      - Address  → dirección física %I/%Q (informativa, para mostrar/loguear)
      - Flag HR  → bit dentro de un %MW (formato "%M<word>.<bit>") al que el
                   programa del PLC copia el estado físico. Es lo que se lee
                   por Modbus FC03, ya que el M221 no expone %I/%Q vía Modbus.
      - TYPE     → INPUT u OUTPUT
    """
    df = pd.read_excel(XLSX_FILE, sheet_name=XLSX_SHEET)
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

        tags.append({
            "address":     address,
            "tag":         symbol,
            "description": comment,
            "type":        typ,
            "mw_word":     word,
            "mw_bit":      bit,
        })

    tags.sort(key=_sort_key)
    return tags
