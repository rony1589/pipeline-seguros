import os
import pandas as pd
from scripts.generar_datos import generar_csvs

def test_generar_datos(tmp_path):
    out_dir = tmp_path / "data"
    out_dir.mkdir()

    pol_path, sin_path = generar_csvs(str(out_dir))

    # Los archivos deben existir
    assert os.path.exists(pol_path)
    assert os.path.exists(sin_path)

    # Leer los CSV generados
    df_pol = pd.read_csv(pol_path)
    df_sin = pd.read_csv(sin_path)

    # Verificar que no estén vacíos
    assert not df_pol.empty
    assert not df_sin.empty

    # Validar columnas esperadas básicas
    assert {"poliza_id", "producto", "prima_mensual"}.issubset(df_pol.columns)
    assert {"siniestro_id", "poliza_id", "monto_reclamado"}.issubset(df_sin.columns)
