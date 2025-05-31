-- Versión ultra-básica sin operaciones complejas
raw_data = LOAD '/input/waze_data.csv' USING TextLoader() AS (line:chararray);

-- Solo mostrar 3 líneas para verificar lectura
limited_data = LIMIT raw_data 3;
DUMP limited_data;