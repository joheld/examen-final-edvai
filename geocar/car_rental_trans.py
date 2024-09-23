from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F

sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.sql import HiveContext
hc = HiveContext(sc)


#lectura

georef = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv")

car_rental = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ",").csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")

#quitar espacios y puntos de los nombres de columna

georef = georef.toDF(*[c.replace(' ', '_').replace('.', '_') for c in georef.columns])
car_rental = car_rental.toDF(*[c.replace(' ', '_').replace('.', '_') for c in car_rental.columns])

#cast de rating a INT

car_rental = car_rental.withColumn("rating", F.round(F.col("rating")).cast("int"))

# join

geo_car = car_rental.join(
    georef, 
    car_rental["location_state"] == georef["United_States_Postal_Service_state_abbreviation"], 
    how="outer"  
)
#eliminar ratings nulos

geo_car = geo_car.dropna(subset=["rating"])

# Convertir los valores de 'fuelType' a minúsculas
#geo_car = geo_car.withColumn("fuelType", F.lower(F.col("fuelType")))

# Excluir las filas donde 'location_state' o 'United_States_Postal_Service_state_abbreviation' sea 'TX'
geo_car = geo_car.filter(
    (geo_car["location_state"] != "TX") & 
    (geo_car["United_States_Postal_Service_state_abbreviation"] != "TX")
)


# drop de columnas que no estaran en hive

columns_to_drop = [
    "location_country", 
    "location_latitude", 
    "location_longitude", 
    "location_state", 
    "vehicle_type", 
    "Geo_Shape", 
    "Year", 
    "Official_Code_State", 
    "Iso_3166-3_Area_Code", 
    "Type", 
    "United_States_Postal_Service_state_abbreviation", 
    "State_FIPS_Code", 
    "State_GNIS_Code", 
    "Geo_Point"  
]

geo_car = geo_car.drop(*columns_to_drop)


# Renombrar las columnas especificadas
geo_car = geo_car.withColumnRenamed("location_city", "city") \
                 .withColumnRenamed("Official_Name_State", "state_name") \
                 .withColumnRenamed("vehicle_make", "make") \
                 .withColumnRenamed("vehicle_model", "model") \
                 .withColumnRenamed("vehicle_year", "year")



# Ordenar las columnas en el orden especificado
geo_car = geo_car.select(
    "fuelType", 
    "rating", 
    "renterTripsTaken", 
    "reviewCount", 
    "city", 
    "state_name", 
    "owner_id", 
    "rate_daily", 
    "make", 
    "model", 
    "year"
)

#cast 

geo_car = geo_car.withColumn("fuelType", F.col("fuelType").cast("string")) \
                 .withColumn("rating", F.col("rating").cast("int")) \
                 .withColumn("renterTripsTaken", F.col("renterTripsTaken").cast("int")) \
                 .withColumn("reviewCount", F.col("reviewCount").cast("int")) \
                 .withColumn("city", F.col("city").cast("string")) \
                 .withColumn("state_name", F.col("state_name").cast("string")) \
                 .withColumn("owner_id", F.col("owner_id").cast("int")) \
                 .withColumn("rate_daily", F.col("rate_daily").cast("int")) \
                 .withColumn("make", F.col("make").cast("string")) \
                 .withColumn("model", F.col("model").cast("string")) \
                 .withColumn("year", F.col("year").cast("int"))


geo_car.write.insertInto('car_rental_db.car_rental_analytics')




