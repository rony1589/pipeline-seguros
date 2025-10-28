
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import os
import pandas as pd

from scripts import validacion, transformaciones, generar_datos
from scripts.utils_auditoria import registrar_evento_auditoria

DEFAULT_ARGS = {
    "owner": "data-engineer",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

DATA_DIR = "/opt/airflow/data"

with DAG(
    "pipeline_polizas",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["demo", "seguros"]
) as dag:

    def preparar_archivos(**ctx):
        inicio = datetime.now()
        date = datetime.now().strftime("%Y%m%d")
        pol_path = os.path.join(DATA_DIR, f"polizas_{date}.csv")
        sin_path = os.path.join(DATA_DIR, f"siniestros_{date}.csv")

        os.makedirs(DATA_DIR, exist_ok=True)
        if not (os.path.exists(pol_path) and os.path.exists(sin_path)):
            p, s = generar_datos.generar_csvs(DATA_DIR, date=datetime.today().date())
            pol_path, sin_path = p, s

        ctx["ti"].xcom_push(key="pol_path", value=pol_path)
        ctx["ti"].xcom_push(key="sin_path", value=sin_path)

        registrar_evento_auditoria("preparar_archivos",
                                archivo=f"{os.path.basename(pol_path)},{os.path.basename(sin_path)}",
                                registros=0, inicio=inicio, estado="OK",
                                mensaje="Archivos preparados/generados")
    preparar = PythonOperator(task_id="preparar_archivos", python_callable=preparar_archivos)

    def validar_archivos_task(**ctx):
        inicio = datetime.now()
        ti = ctx["ti"]
        pol = ti.xcom_pull(key="pol_path")
        sin = ti.xcom_pull(key="sin_path")
        res = validacion.validar_archivos(pol, sin, umbral=0.10)
        ti.xcom_push(key="validacion_resumen", value={
            "estado": res["estado"],
            "pol_total": res["pol_total"],
            "pol_invalidos": res["pol_invalidos"],
            "sin_total": res["sin_total"],
            "sin_invalidos": res["sin_invalidos"]
        })
        # salvar invalidos para cuarentena
        invalid_pol_path = os.path.join("/tmp", "invalid_polizas.csv")
        invalid_sin_path = os.path.join("/tmp", "invalid_siniestros.csv")
        res["invalid_pol_df"].to_csv(invalid_pol_path, index=False)
        res["invalid_sin_df"].to_csv(invalid_sin_path, index=False)
        ti.xcom_push(key="invalid_pol_path", value=invalid_pol_path)
        ti.xcom_push(key="invalid_sin_path", value=invalid_sin_path)

        registrar_evento_auditoria("validar_archivos",
                                archivo=f"{os.path.basename(pol)},{os.path.basename(sin)}",
                                registros=res["pol_total"] + res["sin_total"],
                                inicio=inicio, estado=res["estado"],
                                mensaje=f"Inválidos pol:{res['pol_invalidos']} sin:{res['sin_invalidos']}")
        return res["estado"]

    validar_archivos = PythonOperator(task_id="validar_archivos", python_callable=validar_archivos_task)

    def decidir_fn(**ctx):
        inicio = datetime.now()
        ti = ctx["ti"]
        resumen = ti.xcom_pull(key="validacion_resumen")
        estado = resumen.get("estado", "CORRUPTO")
        if estado == "VALIDO":
            registrar_evento_auditoria("decidir_procesar", inicio=inicio, estado="OK", mensaje="Procesar")
            return "transformar"
        else:
            registrar_evento_auditoria("decidir_procesar", inicio=inicio, estado="CORRUPTO", mensaje="Cuarentena")
            return "mover_cuarentena"

    decidir = BranchPythonOperator(task_id="decidir_procesar", python_callable=decidir_fn)

    def transformar_task(**ctx):
        inicio = datetime.now()
        ti = ctx["ti"]
        pol = ti.xcom_pull(key="pol_path")
        sin = ti.xcom_pull(key="sin_path")
        resumen_file, audit_file = transformaciones.main(pol, sin, DATA_DIR)
        ti.xcom_push(key="resumen_file", value=resumen_file)
        ti.xcom_push(key="audit_file", value=audit_file)
        registrar_evento_auditoria("transformar", archivo=os.path.basename(resumen_file), registros=0, inicio=inicio, estado="OK")
    transformar = PythonOperator(task_id="transformar", python_callable=transformar_task)

    def cargar_bq_task(**ctx):
        inicio = datetime.now()
        ti = ctx["ti"]
        resumen = ti.xcom_pull(key="resumen_file")
        registrar_evento_auditoria("cargar_bq", archivo=os.path.basename(resumen), registros=0, inicio=inicio, estado="OK",
                                mensaje="Carga simulada")
    cargar_bq = PythonOperator(task_id="cargar_bq", python_callable=cargar_bq_task)

    def auditoria_task(**ctx):
        inicio = datetime.now()
        audit_path = os.path.join(DATA_DIR, "auditoria_proceso.csv")
        if os.path.exists(audit_path):
            df = pd.read_csv(audit_path)
            print("AUDITORÍA (últimas líneas):")
            print(df.tail(5).to_string(index=False))
        registrar_evento_auditoria("auditoria", archivo="auditoria_proceso.csv", registros=0, inicio=inicio, estado="OK")
    auditoria = PythonOperator(task_id="auditoria", python_callable=auditoria_task)

    def mover_cuarentena_task(**ctx):
        inicio = datetime.now()
        quarant_dir = "/opt/airflow/quarantine"
        os.makedirs(quarant_dir, exist_ok=True)
        # movemos con la lógica de tu DAG (aquí solo regist.)
        registrar_evento_auditoria("mover_cuarentena", archivo=quarant_dir, registros=0, inicio=inicio, estado="CORRUPTO",
                                mensaje="Archivos a cuarentena")
    mover_cuarentena = PythonOperator(task_id="mover_cuarentena", python_callable=mover_cuarentena_task)

    notificar_exito = BashOperator(task_id="notificar_exito", bash_command='echo "Proceso finalizado OK"')
    notificar_error = BashOperator(task_id="notificar_error", bash_command='echo "Proceso marcado CORRUPTO" && exit 0')

    preparar >> validar_archivos >> decidir
    decidir >> transformar >> cargar_bq >> auditoria >> notificar_exito
    decidir >> mover_cuarentena >> notificar_error
