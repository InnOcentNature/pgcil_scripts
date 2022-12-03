import csv
import json
import logging
import os
import threading
from kafka import KafkaConsumer

ABSOLUTE_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "node_outage_data"
DATA_DIRECTORY = os.path.join(ABSOLUTE_PATH, RELATIVE_PATH)
if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)


class NodeOutageConsumer(threading.Thread):
    daemon = True

    def run(self):
        print("Node-Outage-Data...")
        consumer = KafkaConsumer(bootstrap_servers='pgcil-iskraemeco.probussense.com:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['node-outage-data'])

        for message in consumer:
            try:
                print(message.value)
                asset_number = message.value['assetNumber']
                timestamp = message.timestamp
                debug_server_time = message.value['debugServerTime']
                node_id = message.value['nodeId']
                node_outage_file = open(DATA_DIRECTORY + "\\" + str(asset_number) + "_" + str(node_id) + ".txt", 'w', encoding="utf-8")
                node_outage_file.write(str(timestamp) + " : " + str(message.value) + '\n')
                node_outage_file.close()
            except Exception as e:
                print(e)


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    try:
        threads = [
            NodeOutageConsumer()
        ]
        for t in threads:
            t.start()
            while True:
                pass
    except Exception as error:
        print(error)
    finally:
        node_outage_files = os.listdir(DATA_DIRECTORY)
        count_file = open(DATA_DIRECTORY + '\\' + 'Count_Outage' + '.csv', 'w', newline='', encoding="utf-8")
        writer = csv.writer(count_file)
        writer.writerow(['Meter Number', 'Node Id'])
        for f in node_outage_files:
            f = f.replace(".txt", "").split("_")
            writer.writerow([f[0], f[1]])
        count_file.close()
        print("Stopped")
