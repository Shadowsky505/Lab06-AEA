import csv
from confluent_kafka import Producer

KAFKA_BROKER = '54.165.130.18:9092'  # Dirección del broker
TOPIC_NAME = 'Topico1'      # Cambia al nombre de tu tópico
CSV_FILE = 'data.csv'              # Cambia al nombre de tu archivo CSV

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    if err:
        print(f"Error al enviar el mensaje: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [partición: {msg.partition()}]")

try:
    with open(CSV_FILE, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file, delimiter=';')
        header = next(csv_reader)  
        print(f"Encabezados: {header}")
        
        for row in csv_reader:
            mensaje = ";".join(row)  
            producer.produce(TOPIC_NAME, value=mensaje, callback=delivery_report)
            
            
            producer.flush()
            print(f"Enviado: {mensaje}")

except FileNotFoundError:
    print(f"Error: El archivo {CSV_FILE} no existe.")
except Exception as e:
    print(f"Error: {e}")

producer.flush()
print("Todos los mensajes han sido enviados.")
