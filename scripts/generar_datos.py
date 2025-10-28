
import os
import sys
from datetime import datetime, date, timedelta
import csv
import uuid
import random

# Fix: allow running as script (python scripts/generar_datos.py)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from faker import Faker
from scripts.utils_auditoria import registrar_evento_auditoria

fake = Faker()

def generar_polizas(out_dir, date_str, n=10000, err_rate=0.05):
    inicio = datetime.now()
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(out_dir, f"polizas_{date_str}.csv")
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["poliza_id","cliente_id","producto","suma_asegurada","prima_mensual","fecha_inicio","estado","region"])
        for i in range(n):
            poliza_id = str(uuid.uuid4())
            cliente_id = fake.random_number(digits=6)
            producto = random.choice(["AUTO", "VIDA", "HOGAR"])
            suma = round(random.uniform(1000, 50000), 2)
            prima = round(random.uniform(50, 2000), 2)
            fecha_inicio = (date.today() - timedelta(days=random.randint(0,365))).isoformat()
            estado = random.choice(["ACTIVA", "CANCELADA"])
            region = random.choice(["CDMX","GDL","MTY"])
            # errores intencionales
            if random.random() < err_rate:
                if random.random() < 0.5:
                    suma = -abs(suma)
                else:
                    prima = 0
            if random.random() < err_rate/2:
                poliza_id = ""
            w.writerow([poliza_id, cliente_id, producto, suma, prima, fecha_inicio, estado, region])
    registrar_evento_auditoria("generar_polizas", archivo=os.path.basename(fname), registros=n, inicio=inicio)
    return fname

def generar_siniestros(out_dir, date_str, poliza_ids, n=2000, err_rate=0.05):
    inicio = datetime.now()
    os.makedirs(out_dir, exist_ok=True)
    fname = os.path.join(out_dir, f"siniestros_{date_str}.csv")
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["siniestro_id","poliza_id","fecha_siniestro","tipo_siniestro","monto_reclamado","estado"])
        for i in range(n):
            sin_id = str(uuid.uuid4())
            # en err_rate pequeñas referenciamos poliza inexistente
            if random.random() < err_rate:
                pol_id = "invalid-" + str(uuid.uuid4())
            else:
                pol_id = random.choice(poliza_ids) if poliza_ids else ""
            fecha_siniestro = (date.today() - timedelta(days=random.randint(0,365))).isoformat()
            tipo = random.choice(["CHOQUE","ROBO","INCENDIO"])
            monto = round(random.uniform(100,20000), 2)
            estado = random.choice(["PENDIENTE","APROBADO","RECHAZADO"])
            if random.random() < err_rate:
                monto = -abs(monto)
            if random.random() < err_rate/10:
                sin_id = ""
            w.writerow([sin_id, pol_id, fecha_siniestro, tipo, monto, estado])
    registrar_evento_auditoria("generar_siniestros", archivo=os.path.basename(fname), registros=n, inicio=inicio)
    return fname

def generar_csvs(out_dir="data", date=None):
    """
    Wrapper que genera polizas (n grandes) y siniestros (m) y devuelve (pol_file, sin_file)
    - out_dir: carpeta donde escribe
    - date: datetime.date o None -> hoy
    """
    if date is None:
        date = date or datetime.today().date()
    if isinstance(date, datetime):
        date = date.date()
    date_str = date.strftime("%Y%m%d")

    pol_file = generar_polizas(out_dir, date_str, n=10000, err_rate=0.05)

    # leer IDs válidos
    poliza_ids = []
    with open(pol_file, encoding="utf-8") as f:
        next(f)  # header
        for row in csv.reader(f):
            if row and row[0]:
                poliza_ids.append(row[0])

    sin_file = generar_siniestros(out_dir, date_str, poliza_ids, n=2000, err_rate=0.05)
    return pol_file, sin_file

if __name__ == "__main__":
    p, s = generar_csvs("data")
    print(f"Generados:\n - {p}\n - {s}")
