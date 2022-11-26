from pyspark.sql import SparkSession
from pyspark.sql.functions import ltrim,rtrim,trim,col
from pyspark.sql.functions import *
from pyspark import SparkContext
import pyspark
from pyspark.sql import Row


''''''
spark = SparkSession.builder.config("spark.jars", "postgresql-42.5.1.jar") \
	.master("local").appName("PySpark_Postgres_test").getOrCreate()



oij_df = spark.read.csv( path="OIJ.csv", sep=",", header=True,quote='"',inferSchema=True,)
inec_df = spark.read.csv( path="INEC.csv", sep=";", header=True,quote='"',inferSchema=True,)

def generate_new_columns(df):
    columns = ["Provincia", "Canton", "Distrito","Población de 15 años y más", "Tasa neta de participación", "Tasa de ocupación", "Tasa de desempleo abierto", "Porcentaje de población económicamente inactiva", "Relación de dependencia económica"]
    new_df = spark.createDataFrame(data =[("","","","","","","","","")], schema = columns)
    Provincia =''
    Canton = ''
    Distrito = ''
    counter = 0
    counter2 = 0
    for row in df.collect():
        if row['Provincia, cantón y distrito'] == None:
            counter += 1
            counter2 += 1
            if counter == 4: 
                counter = 2  

            if counter2 == 2:
                counter = 1
            continue
        if counter == 1:
            Provincia = row[0]
        if counter == 2:
            Canton = row[0]
        if counter == 3:
            Distrito = row[0]
            poblacionMayor15 = row[1]
            tasaParticipacion = row[2]
            tasaOcupacion = row[3]
            tasaDesempleo = row[4]
            poblacionInactiva = row[5]
            relacionDependencia = row[6]
            NewRow = (Provincia, Canton, Distrito, poblacionMayor15, tasaParticipacion, tasaOcupacion, tasaDesempleo, poblacionInactiva, relacionDependencia)
            new_df = new_df.union(spark.createDataFrame(data =[NewRow], schema = columns))
        counter2 = 0
    
    return new_df
df = generate_new_columns(inec_df)


'''
url = "jdbc:postgresql://localhost:5432/etl"
mode = "overwrite"
properties = {"user": "postgres", " password": "Legolas00", "driver": "org.postgresql.Driver"}

df.write.jdbc(url=url, table="test", mode=mode, properties=properties)
'''