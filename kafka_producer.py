from kafka import KafkaProducer
import json, time, random, datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensores = ['Centro','Industrial','Norte','Sur','Sur-Oriental']
print("Enviando a topic sensor_data en Sogamoso...")

for i in range(100):
    datos = {
        'sensor_id': random.choice(sensores),
        'temperatura': round(random.uniform(5, 40), 2),
        'humedad': round(random.uniform(30, 90), 2),
        'timestamp': datetime.datetime.now().isoformat()
    }
    producer.send('sensor_data', value=datos)
    print(datos)
    time.sleep(1)
