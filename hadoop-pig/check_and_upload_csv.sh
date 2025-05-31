#!/bin/bash

CSV_LOCAL="/data/datos.csv"
HDFS_DIR="/input"
HDFS_PATH="$HDFS_DIR/datos.csv"

# Función para subir el archivo
upload_to_hdfs() {
    echo "⬆️ Subiendo archivo a HDFS..."
    hdfs dfs -put -f "$CSV_LOCAL" "$HDFS_PATH"
    echo "📃 Listando archivos en $HDFS_DIR:"
    hdfs dfs -ls "$HDFS_DIR"
    echo "✅ Proceso de subida completo."
}

# Verificación inicial
echo "📄 Verificando existencia del archivo CSV local: $CSV_LOCAL"
if [ ! -f "$CSV_LOCAL" ]; then
  echo "❌ Archivo CSV no encontrado en $CSV_LOCAL. Esperando..."
  exit 1
fi

echo "🔌 Probando conexión a HDFS..."
until hdfs dfs -ls /; do
    echo "🔄 Esperando que HDFS esté listo..."
    sleep 5
done

echo "📁 Verificando existencia del directorio en HDFS: $HDFS_DIR"
if ! hdfs dfs -test -d "$HDFS_DIR"; then
  echo "📂 Directorio no existe. Creándolo..."
  hdfs dfs -mkdir -p "$HDFS_DIR"
else
  echo "📂 Directorio ya existe."
fi

# Subir inicialmente
upload_to_hdfs

# Monitorear cambios (opcional)
echo "👀 Monitoreando cambios en el archivo CSV..."
while true; do
    inotifywait -e modify,create "$CSV_LOCAL"
    echo "🔄 Archivo modificado. Volviendo a subir..."
    upload_to_hdfs
    
    # Aquí podrías agregar el procesamiento con Pig
    echo "🐷 Procesando con Pig..."
    pig -x mapreduce -f /path/to/your/script.pig
done