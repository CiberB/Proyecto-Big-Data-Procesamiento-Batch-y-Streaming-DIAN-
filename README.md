# Proyecto Big Data - Procesamiento Batch y Streaming (DIAN)

## 📌 Descripción

Este proyecto implementa un flujo de procesamiento de datos en dos enfoques:

- **Batch**: procesamiento de datos históricos
- **Streaming**: procesamiento de datos en tiempo real usando Kafka y Spark Streaming

Se simula la generación de documentos electrónicos tipo DIAN (facturas, notas crédito, etc.) y se procesan para obtener métricas en tiempo real.

---

## ⚙️ Tecnologías utilizadas

- Apache Hadoop (procesamiento batch)
- Apache Spark
- Apache Kafka
- Python

---

## 🚀 Ejecución del proyecto

### 1. Iniciar Zookeeper

```bash
/opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
