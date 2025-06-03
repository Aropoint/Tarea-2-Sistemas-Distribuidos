#!/bin/bash

# Configuración
HADOOP_HOME=/opt/hadoop
PIG_HOME=/opt/pig
DATA_DIR=/data
HDFS_INPUT=/input
HDFS_OUTPUT=/output
PIG_SCRIPT=/scripts/process_waze_alerts.pig
CSV_FILE=datos_clean.json
HDFS_FILE=waze_data.json

# 1. Iniciar servicios SSH y Hadoop con verificación explícita
echo "⚙️ Iniciando servicios..."
sudo service ssh start

# Configuración crítica para Hadoop en Docker
echo "🔧 Configurando Hadoop para entorno Docker..."
sed -i 's/<\/configuration>/<property><name>dfs.client.use.datanode.hostname<\/name><value>false<\/value><\/property><\/configuration>/' $HADOOP_HOME/etc/hadoop/hdfs-site.xml

# Formatear HDFS solo si no existe
echo "🗄️ Formateando HDFS (si es necesario)..."
if [ ! -d "$HADOOP_HOME/data/namenode" ]; then
  $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
fi

# Iniciar servicios Hadoop con más logs
echo "🚀 Iniciando DFS..."
$HADOOP_HOME/sbin/start-dfs.sh

# Espera crítica para datanodes - Versión mejorada
echo "⏳ Esperando registro de DataNodes..."
DATANODE_READY=false
for i in {1..10}; do
  if $HADOOP_HOME/bin/hdfs dfsadmin -report 2>&1 | grep -q "Live datanodes"; then
    DATANODE_READY=true
    break
  fi
  echo "Intento $i/10: DataNodes no listos, esperando..."
  sleep 10
done

if [ "$DATANODE_READY" = false ]; then
  echo "✗ Error: No se detectaron DataNodes activos después de 10 intentos"
  echo "⚠️ Mostrando reporte de estado:"
  $HADOOP_HOME/bin/hdfs dfsadmin -report
  exit 1
fi

echo "🚀 Iniciando YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

# 2. Configurar estructura HDFS con verificación
echo "📚 Configurando estructura HDFS..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_INPUT || echo "⚠️ No se pudo crear $HDFS_INPUT"
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_OUTPUT || echo "⚠️ No se pudo crear $HDFS_OUTPUT"
$HADOOP_HOME/bin/hdfs dfs -chmod -R 755 $HDFS_INPUT
$HADOOP_HOME/bin/hdfs dfs -chmod -R 755 $HDFS_OUTPUT

# 3. Esperar archivo CSV con timeout
echo "🔍 Esperando archivo CSV..."
CSV_TIMEOUT=60
CSV_WAITED=0
while [ ! -f "$DATA_DIR/$CSV_FILE" ] && [ $CSV_WAITED -lt $CSV_TIMEOUT ]; do
  echo "⏳ Esperando que $CSV_FILE esté disponible... ($CSV_WAITED/$CSV_TIMEOUT segundos)"
  sleep 5
  CSV_WAITED=$((CSV_WAITED + 5))
done

if [ ! -f "$DATA_DIR/$CSV_FILE" ]; then
  echo "✗ Error: Archivo $CSV_FILE no disponible después de $CSV_TIMEOUT segundos"
  exit 1
fi

# 4. Subir archivo a HDFS con verificación mejorada
MAX_RETRIES=5
RETRY_COUNT=0
UPLOAD_SUCCESS=false

while [ $RETRY_COUNT -lt $MAX_RETRIES ] && [ "$UPLOAD_SUCCESS" = false ]; do
  echo "⬆️ Subiendo archivo a HDFS (Intento $((RETRY_COUNT+1))/$MAX_RETRIES)..."
  
  # Verificar espacio en HDFS
  HDFS_SPACE=$($HADOOP_HOME/bin/hdfs dfs -df | awk '{print $4}' | tail -1)
  FILE_SIZE=$(du -b "$DATA_DIR/$CSV_FILE" | awk '{print $1}')
  
  if [ "$HDFS_SPACE" -lt "$FILE_SIZE" ]; then
    echo "✗ Espacio insuficiente en HDFS (Necesario: $FILE_SIZE, Disponible: $HDFS_SPACE)"
    exit 1
  fi

  # Intento de subida
  $HADOOP_HOME/bin/hdfs dfs -put -f "$DATA_DIR/$CSV_FILE" "$HDFS_INPUT/$HDFS_FILE"
  
  if [ $? -eq 0 ]; then
    HDFS_SIZE=$($HADOOP_HOME/bin/hdfs dfs -du -s "$HDFS_INPUT/$HDFS_FILE" | awk '{print $1}')
    LOCAL_SIZE=$(du -b "$DATA_DIR/$CSV_FILE" | awk '{print $1}')

    if [ "$HDFS_SIZE" -eq "$LOCAL_SIZE" ]; then
      echo "✓ Archivo subido correctamente ($HDFS_SIZE bytes)"
      UPLOAD_SUCCESS=true
    else
      echo "✗ Los tamaños no coinciden (HDFS: $HDFS_SIZE vs Local: $LOCAL_SIZE)"
      $HADOOP_HOME/bin/hdfs dfs -rm -f "$HDFS_INPUT/$HDFS_FILE"
    fi
  else
    echo "✗ Falló el intento $((RETRY_COUNT+1))"
    $HADOOP_HOME/bin/hdfs dfs -rm -f "$HDFS_INPUT/$HDFS_FILE" 2>/dev/null
  fi

  RETRY_COUNT=$((RETRY_COUNT+1))
  sleep 10
done

if [ "$UPLOAD_SUCCESS" = false ]; then
  echo "✗ Error: No se pudo subir el archivo después de $MAX_RETRIES intentos"
  echo "⚠️ Estado de HDFS:"
  $HADOOP_HOME/bin/hdfs dfsadmin -report
  exit 1
fi

# 5. Configurar entorno Pig
echo "⚙️ Configurando entorno Pig..."
export PIG_CLASSPATH=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/*

# 6. Esperar que YARN esté listo con verificación
echo "⏳ Esperando que YARN esté listo..."
YARN_READY=false
for i in {1..10}; do
  if $HADOOP_HOME/bin/yarn node -list 2>/dev/null | grep -q "RUNNING"; then
    YARN_READY=true
    break
  fi
  echo "Intento $i/10: YARN no listo, esperando..."
  sleep 10
done

if [ "$YARN_READY" = false ]; then
  echo "✗ Error: YARN no está listo después de 10 intentos"
  echo "⚠️ Mostrando estado de YARN:"
  $HADOOP_HOME/bin/yarn node -list
  exit 1
fi

# 7. Ejecutar script Pig
echo "🐷 Ejecutando script Pig..."
$PIG_HOME/bin/pig -x mapreduce -f "$PIG_SCRIPT"
PIG_EXIT_CODE=$?

if [ $PIG_EXIT_CODE -ne 0 ]; then
  echo "✗ Error en la ejecución de Pig (Código: $PIG_EXIT_CODE)"
  # 8. Mantener contenedor activo
echo "✓ Procesamiento completado. Contenedor activo..."
tail -f /dev/null
fi

# 8. Mantener contenedor activo
echo "✓ Procesamiento completado. Contenedor activo..."
tail -f /dev/null