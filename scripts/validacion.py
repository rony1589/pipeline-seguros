
import os
import sys
from datetime import datetime
import pandas as pd

# allow running directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils_auditoria import registrar_evento_auditoria

EXPECTED_POL_COLS = ["poliza_id","cliente_id","producto","suma_asegurada","prima_mensual","fecha_inicio","estado","region"]
EXPECTED_SIN_COLS = ["siniestro_id","poliza_id","fecha_siniestro","tipo_siniestro","monto_reclamado","estado"]

def validar_polizas_df(df: pd.DataFrame):
    # esquema
    for c in EXPECTED_POL_COLS:
        if c not in df.columns:
            raise ValueError(f"Columna faltante en polizas: {c}")
    conds_ok = (
        df["poliza_id"].notna() & (df["poliza_id"].astype(str).str.len()>0)
        & df["suma_asegurada"].notna() & (pd.to_numeric(df["suma_asegurada"], errors="coerce")>0)
        & df["prima_mensual"].notna() & (pd.to_numeric(df["prima_mensual"], errors="coerce")>0)
    )
    validos = df[conds_ok].copy()
    invalidos = df[~conds_ok].copy()
    return validos, invalidos

def validar_siniestros_df(df: pd.DataFrame, poliza_set:set):
    for c in EXPECTED_SIN_COLS:
        if c not in df.columns:
            raise ValueError(f"Columna faltante en siniestros: {c}")
    conds_ok = (
        df["siniestro_id"].notna() & (df["siniestro_id"].astype(str).str.len()>0)
        & df["poliza_id"].notna() & (df["poliza_id"].astype(str).str.len()>0)
        & df["monto_reclamado"].notna() & (pd.to_numeric(df["monto_reclamado"], errors="coerce")>0)
        & df["poliza_id"].isin(poliza_set)
    )
    validos = df[conds_ok].copy()
    invalidos = df[~conds_ok].copy()
    return validos, invalidos

def validar_archivos(pol_path, sin_path, umbral=0.10):
    """
    Valida ambos archivos y registra auditoría por archivo (polizas, siniestros) + resumen.
    Devuelve la misma estructura `res` que antes para compatibilidad con el DAG/tests.
    """
    inicio_total = datetime.now()
    res = {
        "pol_path": pol_path,
        "sin_path": sin_path,
        "pol_total": 0,
        "pol_invalidos": 0,
        "sin_total": 0,
        "sin_invalidos": 0,
        "estado": "VALIDO",
        "valid_pol_df": None,
        "invalid_pol_df": None,
        "valid_sin_df": None,
        "invalid_sin_df": None
    }

    # --- Leer archivos ---
    pol_df = pd.read_csv(pol_path)
    sin_df = pd.read_csv(sin_path)
    res["pol_total"] = len(pol_df)
    res["sin_total"] = len(sin_df)

    # --- Validar pólizas por separado ---
    inicio_pol = datetime.now()
    valid_pol, invalid_pol = validar_polizas_df(pol_df)
    res["pol_invalidos"] = len(invalid_pol)
    res["valid_pol_df"] = valid_pol
    res["invalid_pol_df"] = invalid_pol

    pct_error_pol = res["pol_invalidos"] / max(1, res["pol_total"])
    estado_pol = "CORRUPTO" if pct_error_pol > umbral else "VALIDO"
    registrar_evento_auditoria(
        etapa="validacion_polizas",
        archivo=os.path.basename(pol_path),
        registros=res["pol_total"],
        inicio=inicio_pol,
        estado=estado_pol,
        mensaje=f"Inválidos: {res['pol_invalidos']} ({pct_error_pol:.2%})"
    )

    # --- Validar siniestros por separado (usa polizas válidas) ---
    inicio_sin = datetime.now()
    pol_set = set(valid_pol["poliza_id"].astype(str).tolist())
    valid_sin, invalid_sin = validar_siniestros_df(sin_df, pol_set)
    res["sin_invalidos"] = len(invalid_sin)
    res["valid_sin_df"] = valid_sin
    res["invalid_sin_df"] = invalid_sin

    pct_error_sin = res["sin_invalidos"] / max(1, res["sin_total"])
    estado_sin = "CORRUPTO" if pct_error_sin > umbral else "VALIDO"
    registrar_evento_auditoria(
        etapa="validacion_siniestros",
        archivo=os.path.basename(sin_path),
        registros=res["sin_total"],
        inicio=inicio_sin,
        estado=estado_sin,
        mensaje=f"Inválidos: {res['sin_invalidos']} ({pct_error_sin:.2%})"
    )

    return res

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--pol", required=True)
    p.add_argument("--sin", required=True)
    p.add_argument("--umbral", type=float, default=0.10)
    args = p.parse_args()
    resumen = validar_archivos(args.pol, args.sin, args.umbral)
    print(f"Estado: {resumen['estado']}")
    print(f"Polizas tot/invalid: {resumen['pol_total']}/{resumen['pol_invalidos']}")
    print(f"Siniestros tot/invalid: {resumen['sin_total']}/{resumen['sin_invalidos']}")
    exit(0 if resumen["estado"]=="VALIDO" else 2)
