# ===============================
# 1. LIBRERÍAS Y SESIÓN SPARK
# ===============================
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('Tarea3_PTS').getOrCreate()

# ===============================
# 2. RUTA EN HDFS
# ===============================
file_path = "hdfs://localhost:9000/Tarea3_Procesamiento_de_Datos_con_Apache_Spark/DocumentosPTSExterno_2026.csv"

# ===============================
# 3. CARGA DE DATOS
# ===============================
df = spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('sep', ';') \
    .load(file_path)

# ===============================
# 4. LIMPIEZA BÁSICA
# ===============================

# Renombrar columnas (evita espacios y errores)
df = df.withColumnRenamed("Identificacion del Receptor", "ID_RECEPTOR") \
       .withColumnRenamed("Receptor", "RECEPTOR") \
       .withColumnRenamed("Identificacion del Emisor", "ID_EMISOR") \
       .withColumnRenamed("Emisor", "EMISOR") \
       .withColumnRenamed("NIT Proveedor Tecnologico", "NIT_PROV") \
       .withColumnRenamed("Proveedor Tecnologico", "PROVEEDOR_TEC") \
       .withColumnRenamed("Cantidad Documentos", "CANT_DOC") \
       .withColumnRenamed("Tipo Documento", "TIPO_DOC") \
       .withColumnRenamed("Fecha Inicio", "FECHA_INICIO") \
       .withColumnRenamed("Fecha Fin", "FECHA_FIN")

# Convertir fechas
df = df.withColumn("FECHA_INICIO", F.to_date("FECHA_INICIO", "dd-MM-yyyy")) \
       .withColumn("FECHA_FIN", F.to_date("FECHA_FIN", "dd-MM-yyyy"))


# ===============================
# 5. EXPLORACIÓN (EDA)
# ===============================

print("🔹 ESQUEMA")
df.printSchema()

print("🔹 PRIMERAS FILAS")
df.show(10)

print("🔹 ESTADÍSTICAS")
df.describe().show()

                                                                                   
# ===============================
# 6. ANÁLISIS DESCRIPTIVO
# ===============================

# Total documentos
print("🔹 TOTAL DOCUMENTOS")
df.select(F.sum("CANT_DOC")).show()

# Top receptores
print("🔹 TOP RECEPTORES")
df.groupBy("RECEPTOR") \
  .agg(F.sum("CANT_DOC").alias("TOTAL_DOC")) \
  .orderBy(F.col("TOTAL_DOC").desc()) \
  .show(10)

# Top emisores
print("🔹 TOP EMISORES")
df.groupBy("EMISOR") \
  .agg(F.sum("CANT_DOC").alias("TOTAL_DOC")) \
  .orderBy(F.col("TOTAL_DOC").desc()) \
  .show(10)

# Top proveedores tecnológicos
print("🔹 TOP PROVEEDORES TECNOLÓGICOS")
df.groupBy("PROVEEDOR_TEC") \
  .agg(F.sum("CANT_DOC").alias("TOTAL_DOC")) \
  .orderBy(F.col("TOTAL_DOC").desc()) \
  .show(10)

# Tipos de documento
print("🔹 TIPOS DE DOCUMENTO")
df.groupBy("TIPO_DOC") \
  .count() \
  .orderBy(F.col("count").desc()) \
  .show()

# ===============================
# 7. FILTROS
# ===============================

print("🔹 DOCUMENTOS MAYORES A 10000")
df.filter(F.col("CANT_DOC") > 10000) \
  .select("EMISOR", "RECEPTOR", "CANT_DOC") \
  .show()

# ===============================
# 8. ORDENAMIENTO
# ===============================

print("🔹 ORDENADOS POR CANTIDAD")
df.orderBy(F.col("CANT_DOC").desc()).show(10)