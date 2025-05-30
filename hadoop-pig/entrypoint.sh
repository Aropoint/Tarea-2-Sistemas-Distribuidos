#!/bin/bash

echo "Inicializando entorno..."

# Crear carpeta temporal de HDFS local
mkdir -p /tmp/hadoop-hadoop
chmod 777 /tmp/hadoop-hadoop

# Formatear HDFS solo si es la primera vez -> esto es como la partición bro
if [ ! -f /tmp/hadoop-hadoop/dfs/name ]; then
  echo "Formateando HDFS..."
  $HADOOP_HOME/bin/hdfs namenode -format -force
fi

echo "Entorno listo."
exec /bin/bash
