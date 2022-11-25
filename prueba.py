from pyspark.sql import SparkSession
from pyspark.sql.functions import ltrim,rtrim,trim,col
from pyspark.sql.functions import *
from pyspark import SparkContext
spark = SparkSession.builder.appName("MyFirstCSV").getOrCreate()

oij_df = spark.read.csv( path="OIJ.csv", sep=",", header=True,quote='"',inferSchema=True,)
inec_df = spark.read.csv( path="INEC.csv", sep=";", header=True,quote='"',inferSchema=True,)

def generate_new_columns(df):
    columns = ["Provincia", "Canton", "Distrito","Tasa neta de participación", "Porcentaje de población económicamente inactiva", "Relación de dependencia económica"]
    new_df = spark.createDataFrame(data =[("","","","","","")], schema = columns)
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

            counter2 = 0
            if counter2 == 2:
                counter = 1
            continue
        if counter == 1:
            Provincia = row[0]
        if counter == 2:
            Canton = row[0]
        if counter == 3:
            Distrito = row[0]
            NewRow = (Provincia, Canton, Distrito, "", "", "")
            new_df = new_df.union(spark.createDataFrame(data =[NewRow], schema = columns))
    
    return new_df
    
generate_new_columns(inec_df).show(200)
