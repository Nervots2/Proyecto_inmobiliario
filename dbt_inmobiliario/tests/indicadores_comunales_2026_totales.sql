-- Este test falla si los indicadores dejan de coincidir con la auditoría validada.
SELECT 1 AS discrepancia
FROM {{ ref('indicadores_comunales_2026') }}
HAVING COUNT(*) <> 346
    OR COALESCE(SUM(publicaciones_total), 0) <> 35188
    OR COALESCE(SUM(predios_total), 0) <> 8560728
    OR COUNT(*) FILTER (WHERE publicaciones_total > 0) <> 92
    OR COUNT(*) FILTER (WHERE unidades_autorizadas_2026 = 0) <> 80
