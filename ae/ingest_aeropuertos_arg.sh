#!/bin/bash

# Definir el directorio de descarga en HDFS
download_dir="hdfs://172.17.0.2:9000/ingest"

# Crear el directorio en HDFS si no existe
hadoop fs -test -d "$download_dir"
if [ $? -ne 0 ]; then
    hadoop fs -mkdir -p "$download_dir"
fi

# Definir un directorio temporal local para almacenar los archivos descargados antes de subirlos a HDFS
temp_local_dir="/home/hadoop/landing_temp"
mkdir -p "$temp_local_dir"

# Descargar el informe del ministerio 2021
echo "Descargando informe del ministerio 2021..."
curl -o "$temp_local_dir/2021-informe-ministerio.csv" https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv

# Descargar el informe del ministerio 2022
echo "Descargando informe del ministerio 2022..."
curl -o "$temp_local_dir/202206-informe-ministerio.csv" https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv

# Descargar el detalle de aeropuertos
echo "Descargando detalles de aeropuertos..."
curl -o "$temp_local_dir/aeropuertos_detalle.csv" https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv

# Subir los archivos descargados a HDFS
hadoop fs -put "$temp_local_dir/2021-informe-ministerio.csv" "$download_dir"
hadoop fs -put "$temp_local_dir/202206-informe-ministerio.csv" "$download_dir"
hadoop fs -put "$temp_local_dir/aeropuertos_detalle.csv" "$download_dir"

# Eliminar el directorio temporal local
rm -rf "$temp_local_dir"

# Confirmar la subida completa
echo "Todos los archivos han sido descargados y subidos a $download_dir en HDFS."