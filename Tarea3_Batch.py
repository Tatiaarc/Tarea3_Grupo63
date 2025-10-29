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


#Top 10 empresas más grandes según ganancias
top10_ganancias = df.select("RAZÓN SOCIAL", "GANANCIA_float","Año de Corte") \
          .sort(F.col("GANANCIA_float").desc()) \
          .limit(10)


print("Top 10 empresas más grandes según ganancias")
top10_ganancias.show()


#Promedio de ingresos operacionales por macrosector en el 2024
df_2024 = df.filter(F.col("Año de Corte") == 2024)

promedio_ingresos_2024 = (
    df_2024.groupBy("Año de Corte", "MACROSECTOR")
    .agg(F.round(F.avg("INGRESOS_float"), 2).alias("promedio_2024"))
    .orderBy(F.col("promedio_2024").desc())
)

print("Promedio de ingresos operacionales por macrosector en el 2024")
promedio_ingresos_2024.show(50)

#Patrimonio anual de las empresas
df_aplicable = df.filter(F.col("Año de Corte").isNotNull())

df_pivot = df_aplicable.groupBy("RAZÓN SOCIAL") \
    .pivot("Año de Corte") \
    .agg(F.round(F.sum("PATRIMONIO_float"), 2))


print("Patromonio anual de las empresas")

df_pivot.show()
