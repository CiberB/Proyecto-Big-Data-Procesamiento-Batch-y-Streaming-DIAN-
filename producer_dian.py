import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

emisores = ["EPS", "SURA", "SANITAS", "COMPENSAR"]
tipos = ["FACTURA", "NOTA_CREDITO", "NOTA_DEBITO"]

while True:
    data = {
        "emisor": random.choice(emisores),
        "tipo_documento": random.choice(tipos),
        "valor": random.randint(10000, 500000),
        "timestamp": int(time.time())
    }

    producer.send('dian_stream', value=data)
    print("Enviado:", data)

    time.sleep(1)