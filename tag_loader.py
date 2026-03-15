import pandas as pd

def load_tags():
    df = pd.read_excel("Programa_TTA_IRSA_convertido.xlsx")
    tags = []
    for _, row in df.iterrows():
        address = str(row["Dirección"]).strip()
        symbol = str(row["Símbolo"]).strip()
        description = str(row["Comentario"]).strip()
        if symbol == "nan":
            continue
        tags.append({
            "address": address,
            "tag": symbol,
            "description": description
        })
    return tags