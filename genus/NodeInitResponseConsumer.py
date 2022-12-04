import json
import os
import threading
import time

from kafka import KafkaConsumer

from master import genus_master_list

ABSOLUTE_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "node_init"
DATA_DIRECTORY = os.path.join(ABSOLUTE_PATH, RELATIVE_PATH)
if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)


class NodeInitConsumer(threading.Thread):
    daemon = True

    def run(self):
        print("node-init-response...")
        consumer = KafkaConsumer(bootstrap_servers='pgcil-genus.probussense.com:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['node-init-response'])

        for message in consumer:
            node_id = str(message.value['nodeId'])
            meterNumber = message.value['meterNumber']
            node_file = open(DATA_DIRECTORY + "\\" + node_id + "_" + meterNumber + ".txt", 'a')
            node_file.write(str(message.value) + '\n')
            node_file.close()
            print(str(message.value))


if __name__ == '__main__':
    threads = [
        NodeInitConsumer()
    ]
    for t in threads:
        t.start()
        while True:
            pass

