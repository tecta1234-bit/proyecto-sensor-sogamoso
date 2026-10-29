# Proyecto Monitoreo IoT Sogamoso - Big Data

**Estudiante:** Sogamoso, Boyacá
**Topic Kafka:** sensor_data
**Partición:** sensor_data-0 (topic con 1 partición)
**Broker:** localhost:9092
**Repositorio:** https://github.com/tecta1234-bit/proyecto-sensor-sogamoso

### Descripción
Productor Kafka simula 5 sensores ambientales de Sogamoso:
- Centro, Industrial, Norte, Sur, Sur-Oriental
Enviando temperatura y humedad cada segundo.

Consumidor Spark Streaming aplica:
- filter(temperatura > 35) para alertas
- groupBy(sensor_id)
- avg(), count()

### Comandos usados en la VM
```bash
# 1. Crear topic
kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1

# 2. Listar
kafka-topics.sh --list --bootstrap-server localhost:9092

# 3. Ver partición
kafka-topics.sh --describe --topic sensor_data --bootstrap-server localhost:9092

# 4. Ejecutar productor
python3 kafka_producer.py

# 5. Ejecutar consumidor
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 spark_consumer.py
