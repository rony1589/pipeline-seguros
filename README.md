# 🏦 Pipeline de Seguros - Data Engineer Challenge

Este proyecto implementa un pipeline **end-to-end** para procesar datos diarios de pólizas y siniestros en una aseguradora.

## 🚀 Objetivo

Procesar archivos CSV diarios (`polizas_YYYYMMDD.csv` y `siniestros_YYYYMMDD.csv`), validar su calidad, transformar los datos y cargarlos en **BigQuery**.

---

## 🧱 Estructura
```
pipeline-seguros/
├── README.md
├── requirements.txt
├── dags/
│ └── pipeline_polizas_dag.py
├── scripts/
│ ├── generar_datos.py
│ ├── validacion.py
│ └── transformaciones.py
│ └── utils_auditoria.py
├── sql/
│ └── create_tables.sql
└── tests/
├── test_generador.py
├── test_validacion.py
└── test_transformaciones.py
```
---

## Requisitos

- Python 3.10/3.11 (3.12 puede funcionar, pero algunos paquetes muestran problemas en Windows)
- Java JDK 11 (para PySpark)
- git, pip
- (Opcional) Airflow 2.6.3 para desplegar DAG

## Instalación local (recomendado usar Git Bash o PowerShell)

```bash
git clone https://github.com/rony1589/pipeline-seguros.git

cd pipeline-seguros

python -m venv .venv

# En Git Bash:
source .venv/Scripts/activate

# En PowerShell:
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt


## 🧪 Ejecución Local

1️⃣ Generar datos:
python -m scripts.generar_datos
# genera files en data/ por defecto

2️⃣ Validar archivos generados:
# reemplaza YYYYMMDD si quieres, o usa los nombres generados
python -m scripts.validacion --pol data/polizas_YYYYMMDD.csv --sin data/siniestros_YYYYMMDD.csv

Ejemplo: python -m scripts.validacion --pol data/polizas_20251027.csv --sin data/siniestros_20251027.csv

3️⃣ Transformar y auditar:
python -m scripts.transformaciones --pol data/polizas_YYYYMMDD.csv --sin data/siniestros_YYYYMMDD.csv --out data

Ejemplo: python -m scripts.transformaciones --pol data/polizas_20251027.csv --sin data/siniestros_20251027.csv --out data


4️⃣  Ejecutar pruebas unitarias
python -m pytest -q
o
python -m pytest -v


## 🧩 Flujo del Pipeline

1. **Generar Datos Sintéticos** → `generar_datos.py`
2. **Validar Estructura y Calidad** → `validacion.py`
3. **Transformar con PySpark/Pandas** → `transformaciones.py`
4. **Ejecutar DAG Airflow** → `pipeline_polizas_dag.py`
5. **Cargar en BigQuery** → `create_tables.sql`

---

## 🧠 Decisiones Técnicas

- **Particionamiento por fecha** en BigQuery → optimiza queries diarias.
- **Validaciones automáticas** → asegura calidad de datos (>10% errores = cuarentena).
- **Branching en Airflow** → si datos corruptos → mover_cuarentena / si válidos → procesar.
- **PySpark local** → procesar y resumir información sin depender de un cluster.

---

🧠 Explicación del Funcionamiento

generar_datos.py: crea CSVs sintéticos con 5% errores simulados.

validacion.py: detecta errores y marca dataset como válido o corrupto.

transformaciones.py: combina pólizas y siniestros, genera resumen por producto.

pipeline_polizas_dag.py: orquesta las tareas diarias en Airflow.

create_tables.sql: define estructura final en BigQuery.

tests/: validan consistencia de generación, validación y transformación.

```
