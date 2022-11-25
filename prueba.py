from pyspark.sql import SparkSession
from pyspark.sql.functions import ltrim,rtrim,trim,col
from pyspark.sql.functions import *
from pyspark import SparkContext
spark = SparkSession.builder.appName("MyFirstCSV").getOrCreate()

oij_df = spark.read.csv( path="OIJ.csv", sep=";", header=True,quote='"',inferSchema=True,)
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
        counter2 = 0
    
    return new_df

inec_df = generate_new_columns(inec_df)

def find_non_matches(df1,df2):
    non_matches = []
    is_match = False
    non_match = ''

    for row in df1.collect():
        for row2 in df2.collect():
            if row['Provincia'] == row2['Provincia'] and row['Canton'] == row2['Canton'] and row['Distrito'] == row2['Distrito']:
                is_match = True
                break
        if is_match == False:
            non_match = row['Provincia'] 

            non_matches.append(non_match)
        is_match = False
    return non_matches

non_matches = find_non_matches(oij_df,inec_df)

for value in non_matches:
    print(value)


