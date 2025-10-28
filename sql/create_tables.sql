-- Tabla: resumen_producto_diario
CREATE TABLE IF NOT EXISTS `project.dataset.resumen_producto_diario` (
  process_date DATE,
  producto STRING,
  total_polizas INT64,
  total_siniestros INT64,
  monto_total NUMERIC,
  prima_promedio NUMERIC
) PARTITION BY process_date;

-- Tabla: auditoria_proceso
CREATE TABLE IF NOT EXISTS `project.dataset.auditoria_proceso` (
  fecha_proceso DATETIME,
  etapa STRING,
  archivo STRING,
  registros INT64,
  estado STRING,
  mensaje STRING,
  duracion_segundos FLOAT64
) PARTITION BY DATE(fecha_proceso);
