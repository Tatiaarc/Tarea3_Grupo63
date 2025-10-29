import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

def generar_factura():
    return {
        "ID_Factura": random.randint(100, 999),
        "Fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "SKU": f"{random.choice(['A','B','C','D'])}{random.randint(100,999)}",
        "Cantidad": random.randint(1, 10),
        "Precio": round(random.uniform(10.0, 100.0), 2)
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    factura_data = generar_factura()
    producer.send('facturas_data', value=factura_data)
    print(f"Sent: {factura_data}")
    time.sleep(1)

