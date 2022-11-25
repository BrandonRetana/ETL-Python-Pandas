import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.config("spark.jars", "postgresql-42.5.1.jar") \
	.master("local").appName("PySpark_Postgres_test").getOrCreate()



df = spark.createDataFrame([
	Row(id=1,name='vijay',marks=67),
	Row(id=2,name='Ajay',marks=88),
	Row(id=3,name='jay',marks=79),
	Row(id=4,name='vinay',marks=67),
])
    
df.show(5)

url = "jdbc:postgresql://localhost:5432/etl"
mode = "overwrite"
properties = {"user": "postgres", " password": "Legolas00", "driver": "org.postgresql.Driver"}

df.write.jdbc(url=url, table="test", mode=mode, properties=properties)
