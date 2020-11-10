from kafka import KafkaProducer

BOOTSTRAP_SERVER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

for _ in range(1):
    print('enviando')
    producer.send('foobar', b'some_message_bytes')
    producer.flush()
