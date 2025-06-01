#!/bin/bash

# Configuración
HADOOP_HOME=/opt/hadoop
PIG_HOME=/opt/pig
DATA_DIR=/data
HDFS_INPUT=/input
HDFS_OUTPUT=/output
PIG_SCRIPT=/scripts/process_waze_alerts.pig
CSV_FILE=datos_clean.csv
HDFS_FILE=waze_data.csv

# 1. Iniciar servicios SSH y Hadoop
echo "⚙️ Iniciando servicios..."
sudo service ssh start
$HADOOP_HOME/bin/hdfs namenode -format -force
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

echo "⏳ Esperando inicialización de HDFS..."
sleep 10

# 2. Configurar estructura HDFS
echo "📚 Configurando estructura HDFS..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_INPUT
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_OUTPUT
$HADOOP_HOME/bin/hdfs dfs -chmod -R 755 $HDFS_INPUT
$HADOOP_HOME/bin/hdfs dfs -chmod -R 755 $HDFS_OUTPUT

# 3. Esperar y verificar archivo CSV
echo "🔍 Esperando archivo CSV..."
while [ ! -f "$DATA_DIR/$CSV_FILE" ]; do
    echo "⏳ Esperando que $CSV_FILE esté disponible..."
    sleep 15
done

# 4. Subir archivo a HDFS con reintentos
MAX_RETRIES=3
RETRY_COUNT=0
UPLOAD_SUCCESS=false

while [ $RETRY_COUNT -lt $MAX_RETRIES ] && [ "$UPLOAD_SUCCESS" = false ]; do
    echo "⬆️ Subiendo archivo a HDFS (Intento $((RETRY_COUNT+1))/$MAX_RETRIES)..."

    $HADOOP_HOME/bin/hdfs dfs -put -f $DATA_DIR/$CSV_FILE $HDFS_INPUT/$HDFS_FILE

    if [ $? -eq 0 ]; then
        HDFS_SIZE=$($HADOOP_HOME/bin/hdfs dfs -du -s $HDFS_INPUT/$HDFS_FILE | awk '{print $1}')
        LOCAL_SIZE=$(du -b $DATA_DIR/$CSV_FILE | awk '{print $1}')

        if [ "$HDFS_SIZE" -eq "$LOCAL_SIZE" ]; then
            echo "✓ Archivo subido correctamente ($HDFS_SIZE bytes)"
            UPLOAD_SUCCESS=true
        else
            echo "✗ Los tamaños no coinciden (HDFS: $HDFS_SIZE vs Local: $LOCAL_SIZE)"
        fi
    else
        echo "✗ Falló el intento $((RETRY_COUNT+1))"
    fi

    RETRY_COUNT=$((RETRY_COUNT+1))
    sleep 5
done

if [ "$UPLOAD_SUCCESS" = false ]; then
    echo "✗ Error: No se pudo subir el archivo después de $MAX_RETRIES intentos"
    exit 1
fi

# 5. Configurar entorno Pig
export PIG_CLASSPATH=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/*

# 6. Esperar que YARN esté listo
echo "⏳ Esperando que YARN esté listo..."
until $HADOOP_HOME/bin/yarn node -list 2> /dev/null | grep -q "RUNNING"; do
    sleep 5
done

# 7. Ejecutar script Pig
echo "🐷 Ejecutando script Pig..."
$PIG_HOME/bin/pig -x mapreduce -f $PIG_SCRIPT

# 8. Mantener contenedor activo
echo "✓ Procesamiento completado. Contenedor activo..."
tail -f /dev/null