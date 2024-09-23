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
       

# Descargar Car Rental Data
echo "Descargando Car Rental Data..."
curl -o "$temp_local_dir/CarRentalData.csv" https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv

# Descargar georef United States of America State
echo "Descargando georef United States of America State..."
curl -o "$temp_local_dir/georef-united-states-of-america-state.csv" https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv

# Subir los archivos descargados a HDFS
hadoop fs -put "$temp_local_dir/CarRentalData.csv" "$download_dir"
hadoop fs -put "$temp_local_dir/georef-united-states-of-america-state.csv" "$download_dir"

# Eliminar el directorio temporal local
rm -rf "$temp_local_dir"

# Confirmar la subida completa
echo "Todos los archivos han sido descargados y subidos a $download_dir en HDFS."
