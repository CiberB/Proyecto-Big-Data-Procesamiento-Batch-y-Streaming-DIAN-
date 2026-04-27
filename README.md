# Proyecto Big Data - Procesamiento Batch y Streaming (DIAN)

## Descripción

Este proyecto implementa un flujo de procesamiento de datos en dos enfoques:

- **Batch**: procesamiento de datos históricos
- **Streaming**: procesamiento de datos en tiempo real usando Kafka y Spark Streaming

Se simula la generación de documentos electrónicos tipo DIAN (facturas, notas crédito, etc.) y se procesan para obtener métricas en tiempo real.

---

## Tecnologías utilizadas

- Apache Hadoop (procesamiento batch)
- Apache Spark
- Apache Kafka
- Python

---

## Ejecución del proyecto

BACH (HADOOP + SPARK)

1. Conexión a la máquina virtual
ssh vboxuser@192.168.64.4
Usuario: vboxuser
Contraseña: bigdata

2. Iniciar Apache Spark
start-master.sh

3. Acceso a Spark Master UI
sudo ss -tunelp | grep 8080

Abrir túnel SSH (nueva terminal):

ssh -L 8080:localhost:8080 hadoop@192.168.64.4

Abrir en navegador:

http://localhost:8080/

4. Iniciar Worker de Spark
start-slave.sh spark://bigdata:7077

5. Iniciar entorno PySpark
pyspark

Abrir Spark UI:

ssh -L 4040:localhost:4040 hadoop@192.168.64.4

Abrir en navegador:

http://localhost:4040/

Salir:

Ctrl + D

6. Acceso a usuario Hadoop
su - hadoop

o

ssh hadoop@192.168.64.4
Contraseña: hadoop

7. Iniciar clúster Hadoop
start-all.sh

Abrir interfaz HDFS:

ssh -L 9870:localhost:9870 hadoop@192.168.64.4
http://localhost:9870/

8. Crear directorio en HDFS
hdfs dfs -mkdir /Tarea3_Procesamiento_de_Datos_con_Apache_Spark

9. Subir dataset

Ejecutar desde tu Mac:

scp "/Users/cristiangiraldo/Documents/Universidad - Ingenieria de Sistemas/Semestre VI (I - 2026)/Big Data/Etapa 3/DocumentosPTSExterno_2026.csv" hadoop@192.168.64.4:/home/hadoop/

10. Procesamiento de datos (Batch)

Ejecutar:

python3 procesamiento_batch.py

¿Qué hace?
Agrupa datos por emisor y tipo de documento
Calcula cantidad de documentos
Calcula valor total

Resultado esperado
+-----------+---------------+---------------------+-------------+
| emisor    | tipo_documento| cantidad_documentos | valor_total |
+-----------+---------------+---------------------+-------------+
| EPS       | FACTURA       | 2                   | 300000      |
| SURA      | NOTA_CREDITO  | 1                   | 50000       |
+-----------+---------------+---------------------+-------------+




STRAMING (KAFKA + SPARK)
1. Iniciar servicios de Kafka
# Iniciar Zookeeper
/opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties

# Iniciar Kafka
/opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties

2. Crear el topic
/opt/Kafka/bin/kafka-topics.sh --create \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--topic dian_stream


3. Ejecutar productor (simulación de datos)
python3 producer_dian.py


4. Ejecutar procesamiento streaming
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
spark_dian_streaming.py


5. Ejecutar procesamiento batch
python3 procesamiento_batch.py


6. Visualización
Consola de Spark Streaming
Interfaz web de Spark:
http://localhost:4040
