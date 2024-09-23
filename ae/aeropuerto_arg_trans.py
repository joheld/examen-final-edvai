from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, split, concat_ws, to_date

sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.sql import HiveContext
hc = HiveContext(sc)

# Lectura de archivos
informe_2021 = spark.read.csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv", sep=";", 
header=True)
informe_2022 = spark.read.csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv", sep=";", header=True)
ae_detalle = spark.read.csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv", sep=";", header=True)

# Unión de 2021 y 2022
informe_2021_2022 = informe_2021.unionByName(informe_2022)

# Transformaciones a informe_2021_2022
informe_2021_2022 = informe_2021_2022.drop("Calidad dato")
informe_2021_2022 = informe_2021_2022.filter(informe_2021_2022["Clasificación Vuelo"].isin("Domestico", "Doméstico"))
informe_2021_2022 = informe_2021_2022.fillna({"Pasajeros": 0})

# Modificación del string de la columna fecha a formato yyyy-MM-dd
informe_2021_2022 = informe_2021_2022.withColumn(
    "fecha",
    concat_ws("-", split(col("fecha"), "/")[2], split(col("fecha"), "/")[1], split(col("fecha"), "/")[0])
)

# Transformaciones ae_detalle
ae_detalle = ae_detalle.drop("inhab", "fir")
ae_detalle = ae_detalle.fillna({"distancia_ref": 0})

# Renombrar columnas de informe_2021_2022
informe_2021_2022 = informe_2021_2022 \
    .withColumnRenamed("Fecha", "fecha") \
    .withColumnRenamed("Hora UTC", "horaUTC") \
    .withColumnRenamed("Clase de Vuelo (todos los vuelos)", "clase_de_vuelo") \
    .withColumnRenamed("Clasificación Vuelo", "clasificacion_de_vuelo") \
    .withColumnRenamed("Tipo de Movimiento", "tipo_de_movimiento") \
    .withColumnRenamed("Aeropuerto", "aeropuerto") \
    .withColumnRenamed("Origen / Destino", "origen_destino") \
    .withColumnRenamed("Aerolinea Nombre", "aerolinea_nombre") \
    .withColumnRenamed("Aeronave", "aeronave") \
    .withColumnRenamed("Pasajeros", "pasajeros")

# Cast de columnas de informe_2021_2022
informe_2021_2022 = informe_2021_2022 \
    .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd")) \
    .withColumn("horaUTC", col("horaUTC").cast("string")) \
    .withColumn("clase_de_vuelo", col("clase_de_vuelo").cast("string")) \
    .withColumn("clasificacion_de_vuelo", col("clasificacion_de_vuelo").cast("string")) \
    .withColumn("tipo_de_movimiento", col("tipo_de_movimiento").cast("string")) \
    .withColumn("aeropuerto", col("aeropuerto").cast("string")) \
    .withColumn("origen_destino", col("origen_destino").cast("string")) \
    .withColumn("aerolinea_nombre", col("aerolinea_nombre").cast("string")) \
    .withColumn("aeronave", col("aeronave").cast("string")) \
    .withColumn("pasajeros", col("pasajeros").cast("int"))

# Renombrar columnas de ae_detalle
ae_detalle = ae_detalle \
    .withColumnRenamed("local", "aeropuerto") \
    .withColumnRenamed("oaci", "oac") \
    .withColumnRenamed("iata", "iata") \
    .withColumnRenamed("tipo", "tipo") \
    .withColumnRenamed("denominacion", "denominacion") \
    .withColumnRenamed("coordenadas", "coordenadas") \
    .withColumnRenamed("latitud", "latitud") \
    .withColumnRenamed("longitud", "longitud") \
    .withColumnRenamed("elev", "elev") \
    .withColumnRenamed("uom_elev", "uom_elev") \
    .withColumnRenamed("ref", "ref") \
    .withColumnRenamed("distancia_ref", "distancia_ref") \
    .withColumnRenamed("direccion_ref", "direccion_ref") \
    .withColumnRenamed("condicion", "condicion") \
    .withColumnRenamed("control", "control") \
    .withColumnRenamed("region", "region") \
    .withColumnRenamed("uso", "uso") \
    .withColumnRenamed("trafico", "trafico") \
    .withColumnRenamed("sna", "sna") \
    .withColumnRenamed("concesionado", "concesionado") \
    .withColumnRenamed("provincia", "provincia")

# Cast de ae_detalle
ae_detalle = ae_detalle \
    .withColumn("aeropuerto", col("aeropuerto").cast("string")) \
    .withColumn("oac", col("oac").cast("string")) \
    .withColumn("iata", col("iata").cast("string")) \
    .withColumn("tipo", col("tipo").cast("string")) \
    .withColumn("denominacion", col("denominacion").cast("string")) \
    .withColumn("coordenadas", col("coordenadas").cast("string")) \
    .withColumn("latitud", col("latitud").cast("string")) \
    .withColumn("longitud", col("longitud").cast("string")) \
    .withColumn("elev", col("elev").cast("float")) \
    .withColumn("uom_elev", col("uom_elev").cast("string")) \
    .withColumn("ref", col("ref").cast("string")) \
    .withColumn("distancia_ref", col("distancia_ref").cast("float")) \
    .withColumn("direccion_ref", col("direccion_ref").cast("string")) \
    .withColumn("condicion", col("condicion").cast("string")) \
    .withColumn("control", col("control").cast("string")) \
    .withColumn("region", col("region").cast("string")) \
    .withColumn("uso", col("uso").cast("string")) \
    .withColumn("trafico", col("trafico").cast("string")) \
    .withColumn("sna", col("sna").cast("string")) \
    .withColumn("concesionado", col("concesionado").cast("string")) \
    .withColumn("provincia", col("provincia").cast("string"))

# Inserción en Hive
informe_2021_2022.write.insertInto('aeropuertos_arg.aeropuerto_tabla')
ae_detalle.write.insertInto('aeropuertos_arg.aeropuerto_detalles_tabla')

# Detener Spark
spark.stop()