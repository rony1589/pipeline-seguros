"""
Utilidad central para auditoría de procesos ETL.
Registra eventos en un archivo CSV estructurado, con timestamp y duración.
"""
import csv
import os
from datetime import datetime

AUDITORIA_FILE = os.path.join("data", "auditoria_proceso.csv")

def registrar_evento_auditoria(etapa, archivo=None, registros=None, estado="OK", mensaje=None, inicio=None):
    os.makedirs(os.path.dirname(AUDITORIA_FILE), exist_ok=True)
    fin = datetime.now()
    duracion = round((fin - inicio).total_seconds(), 2) if inicio else None

    registro = {
        "fecha_proceso": fin.strftime("%Y-%m-%d %H:%M:%S"),
        "etapa": etapa,
        "archivo": archivo or "",
        "registros": registros or 0,
        "estado": estado,
        "mensaje": mensaje or "",
        "duracion_segundos": duracion or 0
    }

    existe = os.path.exists(AUDITORIA_FILE)
    with open(AUDITORIA_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=registro.keys())
        if not existe:
            writer.writeheader()
        writer.writerow(registro)
