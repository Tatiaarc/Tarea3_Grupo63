#Importe de librerias
from pyspark.sql import SparkSession, functions as F
# Inicializacion de sesión de Spark
spark = SparkSession.builder.appName('Tarea3_Grupo63').getOrCreate()
# Definicion de ruta para lectura de archivo
file_path = 'hdfs://localhost:9000/Tarea3_Grupo63/rows.csv.1'
# Lectura de archivo .csv
df = spark.read.format('csv') \
    .option('header','true') \
    .option('inferSchema','true') \
    .option('mode','DROPMALFORMED') \
    .load(file_path)

#Conteo de empresas en el deocumento:

conteo_nit = df.select("NIT").distinct().count()

print("Cantidad de empresas: ")
print(conteo_nit)

#Limpieza de columna de Ganancia, se retira simbolo $ y se reemplaza coma por punto y se convierte a float
#para poder hacer operaciones numericas

df = df.withColumn("ganancia_sin_signo", F.regexp_replace("`GANANCIA (PÉRDIDA)`", "[$]", ""))

df = df.withColumn("GANANCIA_decimal", F.regexp_replace("ganancia_sin_signo", ",", "."))

df = df.withColumn("GANANCIA_float", F.col("GANANCIA_decimal").cast("float"))


#Limpieza de columna de Ingresos Operacionales, se retira simbolo $ y se reemplaza coma por punto y se convierte a float
#para poder hacer operaciones numericas

df = df.withColumn("ingresos_sin_signo", F.regexp_replace("INGRESOS OPERACIONALES", "[$]", ""))

df = df.withColumn("INGRESOS_decimal", F.regexp_replace("ingresos_sin_signo", ",", "."))

df = df.withColumn("INGRESOS_float", F.col("INGRESOS_decimal").cast("float"))


#Limpieza de columna de Patrimonio, se retira simbolo $ y se reemplaza co>
#para poder hacer operaciones numericas

df = df.withColumn("patrimonio_sin_signo", F.regexp_replace("TOTAL PATRIMONIO", "[$]", ""))

df = df.withColumn("PATRIMONIO_decimal", F.regexp_replace("patrimonio_sin_signo", ",", "."))

df = df.withColumn("PATRIMONIO_float", F.col("INGRESOS_decimal").cast("float"))
