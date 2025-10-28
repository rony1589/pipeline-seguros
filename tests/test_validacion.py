import pandas as pd
from scripts.validacion import validar_polizas_df, validar_siniestros_df

def test_validar_polizas():
    df = pd.DataFrame({
        "poliza_id": ["p1", "p2", ""],
        "cliente_id": [1, 2, 3],
        "producto": ["AUTO", "VIDA", "HOGAR"],
        "suma_asegurada": [10000, -5, 2000],
        "prima_mensual": [300, 400, 0],
        "fecha_inicio": ["2024-01-01", "2024-02-01", "2024-03-01"],
        "estado": ["ACTIVA", "ACTIVA", "CANCELADA"],
        "region": ["CDMX", "GDL", "MTY"]
    })

    validos, invalidos = validar_polizas_df(df)

    assert len(validos) == 1
    assert len(invalidos) == 2


def test_validar_siniestros():
    df = pd.DataFrame({
        "siniestro_id": ["s1", "s2", ""],
        "poliza_id": ["p1", "nonex", "p1"],
        "fecha_siniestro": ["2024-01-01", "2024-02-01", "2024-03-01"],
        "tipo_siniestro": ["CHOQUE", "ROBO", "INCENDIO"],
        "monto_reclamado": [1000, -5, 500],
        "estado": ["APROBADO", "PENDIENTE", "RECHAZADO"]
    })

    polizas_validas = {"p1"}
    validos, invalidos = validar_siniestros_df(df, polizas_validas)

    assert len(validos) == 1
    assert len(invalidos) == 2
