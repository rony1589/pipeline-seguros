import pandas as pd
from scripts.transformaciones import resumen_por_producto

def test_resumen_por_producto():
    pol = pd.DataFrame({
        "poliza_id": ["p1", "p2", "p3"],
        "cliente_id": [1, 2, 3],
        "producto": ["AUTO", "AUTO", "VIDA"],
        "suma_asegurada": [10000, 20000, 30000],
        "prima_mensual": [100, 200, 300],
        "fecha_inicio": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "estado": ["ACTIVA", "ACTIVA", "ACTIVA"],
        "region": ["CDMX", "GDL", "MTY"]
    })

    sin = pd.DataFrame({
        "siniestro_id": ["s1", "s2"],
        "poliza_id": ["p1", "p3"],
        "fecha_siniestro": ["2024-02-01", "2024-02-02"],
        "tipo_siniestro": ["CHOQUE", "ROBO"],
        "monto_reclamado": [1000, 2000],
        "estado": ["APROBADO", "PENDIENTE"]
    })

    resumen = resumen_por_producto(pol, sin)

    # Validar productos esperados
    assert set(resumen["producto"].tolist()) == {"AUTO", "VIDA"}

    # Validar conteos
    auto = resumen[resumen["producto"] == "AUTO"].iloc[0]
    vida = resumen[resumen["producto"] == "VIDA"].iloc[0]

    assert auto["total_polizas"] == 2
    assert vida["total_polizas"] == 1

    # Validar tipos de datos numéricos
    assert resumen["monto_total"].dtype in ["float64", "float32"]
    assert resumen["total_polizas"].dtype == "int64"
