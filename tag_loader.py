import re
import pandas as pd

def _address_sort_key(tag):
    """
    Ordena primero %I luego %Q, y dentro de cada grupo por word y bit.
    Cualquier dirección que no matchee el patrón va al final.
    """
    m = re.match(r'%([IQ])(\d+)\.(\d+)', tag["address"])
    if not m:
        return (2, 0, 0)
    prefix, word, bit = m.group(1), int(m.group(2)), int(m.group(3))
    return (0 if prefix == "I" else 1, word, bit)

def load_tags():
    df = pd.read_excel("Programa_TTA_IRSA_convertido.xlsx")
    tags = []
    for _, row in df.iterrows():
        address = str(row.get("Dirección", row.get("Address", "nan"))).strip()
        symbol = str(row.get("Símbolo", row.get("Symbol", "nan"))).strip()
        description = str(row.get("Comentario", row.get("Comment", "nan"))).strip()
        if address == "nan" or not address.startswith("%"):
            continue
        if address.startswith("%TM"):
            continue
        if symbol == "nan" or not symbol:
            symbol = address
        if description == "nan":
            description = ""
        tags.append({
            "address": address,
            "tag": symbol,
            "description": description
        })
    tags.sort(key=_address_sort_key)
    return tags