import json
import logging
import threading
import time
from datetime import date
import os
from kafka import KafkaConsumer

ABSOLUTE_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "data"
DATA_DIRECTORY = os.path.join(ABSOLUTE_PATH, RELATIVE_PATH)
if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        print("raw-sensor-data topic...")
        consumer = KafkaConsumer(bootstrap_servers='pgcil-hpl.probussense.com:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['raw-sensor-data'])

        for message in consumer:
            node_id = str(message.value['nodeId'])
            today = date.today()
            directory_path = os.path.join(DATA_DIRECTORY, str(today))
            directory_exists = os.path.exists(directory_path)
            if not directory_exists:
                os.mkdir(directory_path)
            folder_name_profile = str(message.value['type'])

            if not os.path.exists(directory_path + '\\' + folder_name_profile):
                os.mkdir(directory_path + '\\' + folder_name_profile)

            if str(message.value['type']) == 'Instant_Profile':
                f = open(directory_path + '\\' + folder_name_profile + '\\' + node_id + '.txt', 'a')
                f.write(str(message.value) + '\n')
                f.close()
            if str(message.value['type']) == 'Event_Profile':
                f = open(directory_path + '\\' + folder_name_profile + '\\' + node_id + '.txt', 'a')
                f.write(str(message.value) + '\n')
                f.close()
            if str(message.value['type']) == 'Load_Profile':
                f = open(directory_path + '\\' + folder_name_profile + '\\' + node_id + '.txt', 'a')
                f.write(str(message.value) + '\n')
                f.close()
            if str(message.value['type']) == 'Billing_Profile':
                f = open(directory_path + '\\' + folder_name_profile + '\\' + node_id + '.txt', 'a')
                f.write(str(message.value) + '\n')
                f.close()
            if str(message.value['type']) == 'Midnight_Profile':
                f = open(directory_path + '\\' + folder_name_profile + '\\' + node_id + '.txt', 'a')
                f.write(str(message.value) + '\n')
                f.close()

            if message.value.get('commandId') is None:
                if message.value['type'] != 'Event_Profile':
                    if len(message.value['dataObis']) == 0 or len(message.value['data'][0]) == 0 or len(
                            message.value['scalarObis']) == 0 or len(message.value['scalar']) == 0:
                        print(f'Incorrect  {str(message.value)}')
                    else:
                        print(f'Correct  {str(message.value)}')
                else:
                    print(f'Correct  {str(message.value)}')
            else:
                if message.value['type'] != 'Event_Profile':
                    if len(message.value['dataObis']) == 0 or len(message.value['data'][0]) == 0 or len(
                            message.value['scalarObis']) == 0 or len(message.value['scalar']) == 0:
                        print(f'on Demand : Incorrect  {str(message.value)}')
                    else:
                        print(f'on Demand : Correct  {str(message.value)}')
                else:
                    print(f'on Demand : Correct  {str(message.value)}')


def main():
    threads = [
        Consumer()
    ]
    for t in threads:
        t.start()
        time.sleep(10)
    while True:
        pass


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    try:
        main()
    except Exception as e:
        print(e)
    finally:
        print('\nConsumer stopped')
