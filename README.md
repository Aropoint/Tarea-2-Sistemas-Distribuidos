# Tarea-2-Sistemas-Distribuidos

## Aaron Pozas Oyarce - Diego Pérez Carrasco

Este proyecto está organizado en carpetas principales:

1. **map-scraper**: Contiene un script que realiza scraping de datos desde Waze. Este script extrae información relevante y manda las alertas hacia la base de datos.

2. **bdd**: Incluye un servidor que consume los datos del scraper y lo utiliza para procesar o mostrar los datos.

3. **redis-cache**: Carpeta que contiene en su interior todo lo relacionado al backend del proyecto, así como tambien lo relacionado a la inserción de datos en el cache.

4. **request**: Carpeta relacionada al generador de tráfico, en ella se encuentra las dos distribuciones **(1 = uniforme, 0 = exponencial)**. Para probar las distintas distribuciones se debe cambiar de forma manual dentro del código.

Además, el proyecto incluye un archivo `docker-compose.yml` que configura y levanta los contenedores de todos estos modulos. Solo es necesario el comando de más abajo para levantar el proyecto, si se requiere cambiar la política de remoción o el tamaño del cache es necesario cambiarlo en el mismo docker-compose.yml, en el contenedor correspondiente a **redis**.

Dentro del docker-compose.yml se encuentran todos los contenedores y asociaciones a los dockerfile correspondientes.

## Instrucciones para inicializar el proyecto

1. Para levantar los contenedores definidos en el archivo `docker-compose.yml`:

   ```bash
   docker-compose up --build
   ```

2. Una vez arriba los contenedores todo comenzará de forma automática, si se observa algún error 404 - 500 en la primera inicialización se debe esperar a que el scrapper termine de scrapear los 10.000 datos (aprox 1 minuto), posterior a esto se cargarán en la base de datos y el generador de tráfico podrá empezar las request dando como resultado peticiónes 200.

3. A medida que avanzan las peticiones se mostrará por consola todo el procedimiento, las métricas se visualizan una vez que finaliza un ciclo. Solo debe esperar a que aparezcan en la terminal para poder visualizar las métricas asociadas a la cantidad de peticiones, hit rate, miss rate, y el porcentaje de este.

4. También, si se quiere, puede ver la base de datos en mongo express llamada **waze_db** en http://localhost:8081.
5. Cabe recalcar, que si se quiere realizar otra prueba, es recommendable antes borrar la base de datos en mongo express, para evitar errores del tráfico.
