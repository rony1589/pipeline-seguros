
import os
import sys
from datetime import datetime
import pandas as pd

# Permitir ejecución directa
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils_auditoria import registrar_evento_auditoria

def resumen_por_producto(polizas_df, siniestros_df):
    """Genera resumen por producto combinando pólizas y siniestros"""
    pol = polizas_df.copy()
    sin = siniestros_df.copy()

    # conversión segura de tipos numéricos
    pol["suma_asegurada"] = pd.to_numeric(pol["suma_asegurada"], errors="coerce").fillna(0)
    pol["prima_mensual"] = pd.to_numeric(pol["prima_mensual"], errors="coerce").fillna(0)
    sin["monto_reclamado"] = pd.to_numeric(sin["monto_reclamado"], errors="coerce").fillna(0)

    merged = pd.merge(pol, sin, on="poliza_id", how="left", suffixes=("_pol", "_sin"))

    total_pol = pol.groupby("producto").agg(total_polizas=("poliza_id", "nunique")).reset_index()
    resumen_sin = merged.groupby("producto").agg(
        total_siniestros=("siniestro_id", "count"),
        monto_total=("monto_reclamado", "sum"),
        prima_promedio=("prima_mensual", "mean")
    ).reset_index()

    resumen = pd.merge(total_pol, resumen_sin, on="producto", how="left").fillna(0)
    resumen["total_polizas"] = resumen["total_polizas"].astype(int)
    resumen["total_siniestros"] = resumen["total_siniestros"].astype(int)
    resumen["monto_total"] = resumen["monto_total"].astype(float)
    resumen["prima_promedio"] = resumen["prima_promedio"].astype(float).round(2)

    return resumen


def main(pol_path, sin_path, out_dir="data"):
    """Ejecuta la transformación y registra auditoría detallada en auditoria_proceso.csv"""
    inicio_total = datetime.now()
    os.makedirs(out_dir, exist_ok=True)

    # --- Lectura de archivos ---
    inicio_lectura = datetime.now()
    pol = pd.read_csv(pol_path)
    sin = pd.read_csv(sin_path)

    # Auditoría individual por archivo de entrada
    registrar_evento_auditoria(
        etapa="lectura_polizas",
        archivo=os.path.basename(pol_path),
        registros=len(pol),
        inicio=inicio_lectura,
        estado="OK",
        mensaje="Archivo pólizas leído correctamente"
    )

    registrar_evento_auditoria(
        etapa="lectura_siniestros",
        archivo=os.path.basename(sin_path),
        registros=len(sin),
        inicio=inicio_lectura,
        estado="OK",
        mensaje="Archivo siniestros leído correctamente"
    )

    # --- Transformación ---
    inicio_tr = datetime.now()
    resumen = resumen_por_producto(pol, sin)
    fecha_tag = inicio_total.strftime("%Y%m%d")
    resumen_file = os.path.join(out_dir, f"resumen_producto_{fecha_tag}.csv")
    resumen.to_csv(resumen_file, index=False)

    # Auditoría del resultado
    registrar_evento_auditoria(
        etapa="transformacion",
        archivo=os.path.basename(resumen_file),
        registros=len(resumen),
        inicio=inicio_tr,
        estado="OK",
        mensaje="Resumen por producto generado correctamente"
    )

    duracion_total = round((datetime.now() - inicio_total).total_seconds(), 2)
    print(f"✅ Transformación completada en {duracion_total}s")
    print(f"📄 Archivo generado: {resumen_file}")

    return resumen_file


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Transformación y auditoría de archivos de pólizas y siniestros")
    p.add_argument("--pol", required=True, help="Ruta del archivo de pólizas")
    p.add_argument("--sin", required=True, help="Ruta del archivo de siniestros")
    p.add_argument("--out", default="data", help="Directorio de salida")
    args = p.parse_args()

    resumen_file = main(args.pol, args.sin, args.out)
    print(f"Resumen generado en: {resumen_file}")
