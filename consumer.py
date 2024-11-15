from confluent_kafka import Consumer, KafkaException, KafkaError

KAFKA_BROKER = '54.165.130.18:9092'  # Dirección del broker
GROUP_ID = 'Consumer1'        # Identificador único del grupo de consumidores
TOPIC_NAME = 'Topico1'     # Cambia al nombre de tu tópico

consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,  # Dirección del broker
    'group.id': GROUP_ID,              # Grupo de consumidores
    'auto.offset.reset': 'earliest',   # Leer desde el inicio si no hay commit previo
    'enable.auto.commit': True         # Commit automático de offsets
}

consumer = Consumer(consumer_config)

consumer.subscribe([TOPIC_NAME])

print(f"Conectado al tópico '{TOPIC_NAME}'. Escuchando mensajes...")

try:
    while True:
        msg = consumer.poll(1.0) 

        if msg is None:
            continue  
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Fin de la partición alcanzado: {msg.topic()} [{msg.partition()}]")
            else:
                raise KafkaException(msg.error())
        else:
            print(f"Mensaje recibido: {msg.value().decode('utf-8')} "
                  f"[tópico: {msg.topic()} | partición: {msg.partition()} | offset: {msg.offset()}]")

except KeyboardInterrupt:
    print("\nCerrando consumidor...")

finally:
    # Cerrar el consumidor al terminar
    consumer.close()
    print("Consumidor cerrado.")
