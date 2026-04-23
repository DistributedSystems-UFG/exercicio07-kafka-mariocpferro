from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

# Create consumer: Option 1 -- only consume new events
consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

# Create consumer: Option 2 -- consume old events (uncomment to test -- and comment Option 1 above)
#consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')

try:
  topic = sys.argv[1]
  output_topic = sys.argv[2]
except:
  print ('Usage: python3 consumer <input_topic> <output_topic>')
  exit(1)

producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

consumer.subscribe([topic])
for msg in consumer:
    print (msg.value)
    processed = b'[PROCESSED] ' + msg.value
    producer.send(output_topic, value=processed)
    producer.flush()
