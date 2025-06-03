-- Registrar las librerías con la ruta completa
REGISTER file:/opt/pig/lib/piggybank.jar;
REGISTER file:/opt/pig/lib/json-simple.jar;

-- 1. Verificar que el archivo JSON existe
raw_test = LOAD '/input/waze_data.json' USING TextLoader() AS (line:chararray);
test_sample = LIMIT raw_test 5;
DUMP test_sample;

-- 2. Cargar el JSON con el loader correcto
-- Opción A: Con esquema explícito
alerts = LOAD '/input/waze_data.json' 
    USING JsonLoader(
        'city:chararray, 
         confidence:chararray,
         country:chararray,
         fromNodeId:chararray,
         id:chararray,
         inscale:chararray,
         magvar:chararray,
         pubMillis:chararray,
         reliability:chararray,
         reportByMunicipalityUser:chararray,
         reportMood:chararray,
         reportRating:chararray,
         roadType:chararray,
         speed:chararray,
         toNodeId:chararray,
         type:chararray,
         uuid:chararray'
    );

-- 3. Verificar la carga
DESCRIBE alerts;
sample_data = LIMIT alerts 5;
DUMP sample_data;

-- 4. Procesamiento (ejemplo)
santiago_alerts = FILTER alerts BY city == 'Santiago';
final_data = FOREACH santiago_alerts GENERATE 
    uuid, 
    type, 
    city, 
    SUBSTRING(pubMillis, 0, 10) AS timestamp;

-- 5. Resultados
DUMP final_data;
STORE final_data INTO '/output/santiago_alerts_processed' USING PigStorage('|');