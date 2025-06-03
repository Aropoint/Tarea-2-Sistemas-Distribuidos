REGISTER '/opt/pig/lib/piggybank.jar';

-- 1. Cargar datos limpios
cleaned = LOAD '/output/cleaned_metrics'
    USING PigStorage(',')
    AS (
        type:chararray,
        city:chararray,
        count:long,
        first_timestamp:long,
        last_timestamp:long
    );

-- 2. Agrupar incidentes por comuna (patrones geográficos)
by_city = GROUP cleaned BY city;
city_metrics = FOREACH by_city GENERATE
    group AS city,
    SUM(cleaned.count) AS total_incidents;

STORE city_metrics INTO '/output/analysis_by_city' USING PigStorage(',');

-- 3. Contar frecuencia de tipos de incidentes
by_type = GROUP cleaned BY type;
type_metrics = FOREACH by_type GENERATE
    group AS type,
    SUM(cleaned.count) AS total_incidents;

STORE type_metrics INTO '/output/analysis_by_type' USING PigStorage(',');

-- 4. Análisis temporal: incidentes por día (usando first_timestamp)
-- Convertir timestamp a día (ejemplo: dividir por 86400000 para obtener el día en epoch)
by_day = FOREACH cleaned GENERATE
    type,
    city,
    (long)(first_timestamp / 86400000) AS day_epoch,
    count;

group_by_day = GROUP by_day BY day_epoch;
day_metrics = FOREACH group_by_day GENERATE
    group AS day_epoch,
    SUM(by_day.count) AS total_incidents;

STORE day_metrics INTO '/output/analysis_by_day' USING PigStorage(',');