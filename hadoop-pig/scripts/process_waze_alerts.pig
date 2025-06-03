-- filepath: d:\Codes\Tarea-2-Sistemas-Distribuidos\hadoop-pig\scripts\process_waze_alerts.pig
REGISTER '/opt/pig/lib/piggybank.jar';

-- 1. Cargar datos desde HDFS
raw_data = LOAD '/input/waze_data.csv'
    USING PigStorage(',')
    AS (
        uuid:chararray,
        type:chararray,
        city:chararray,
        street:chararray,
        speed:chararray,
        reliability:chararray,
        confidence:chararray,
        country:chararray,
        reportRating:chararray,
        pubMillis:chararray,
        additionalInfo:chararray,
        fromNodeId:chararray,
        id:chararray,
        inscale:chararray,
        magvar:chararray,
        nComments:chararray,
        nThumbsUp:chararray,
        nearBy:chararray,
        provider:chararray,
        providerId:chararray,
        reportBy:chararray,
        reportByMunicipalityUser:chararray,
        reportDescription:chararray,
        reportMood:chararray,
        roadType:chararray,
        subtype:chararray,
        toNodeId:chararray
    );

-- 2. Eliminar duplicados exactos
deduped = DISTINCT raw_data;

-- 3. Filtrar registros válidos y normalizar campos clave
clean_data = FILTER deduped BY (uuid IS NOT NULL AND type IS NOT NULL AND city IS NOT NULL AND pubMillis IS NOT NULL);
homogenized = FOREACH clean_data GENERATE
    uuid,
    UPPER(TRIM(type)) AS type_norm,
    UPPER(TRIM(city)) AS city_norm,
    (long)pubMillis AS timestamp,
    TRIM(reportDescription) AS description;

-- 4. Agrupar por tipo y comuna (esquema unificado)
grouped = GROUP homogenized BY (type_norm, city_norm);
unified = FOREACH grouped GENERATE
    FLATTEN(group) AS (type, city),
    COUNT(homogenized) AS count,
    MIN(homogenized.timestamp) AS first_timestamp,
    MAX(homogenized.timestamp) AS last_timestamp;

-- 5. Guardar resultados en HDFS
STORE unified INTO '/output/cleaned_metrics' USING PigStorage(',');