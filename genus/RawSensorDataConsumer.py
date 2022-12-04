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

today = date.today()
slot_dict = {}


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        print("raw-sensor-data topic...")
        consumer = KafkaConsumer(bootstrap_servers='pgcil-genus.probussense.com:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['raw-sensor-data'])

        for message in consumer:
            # print(message)
            node_id = str(message.value['nodeId'])
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
                # print("Instant :", str(message.value))
            if str(message.value['type']) == 'Event_Profile':
                f = open(directory_path + '\\' + folder_name_profile + '\\' + node_id + '.txt', 'a')
                f.write(str(message.value) + '\n')
                f.close()
            if str(message.value['type']) == 'Load_Profile':
                data_obis = message.value['dataObis']
                data = message.value['data']
                scalar_obis = message.value['scalarObis']
                scalar = message.value['scalar']
                condition = len(data_obis) < 1 or len(data) < 1 or len(scalar_obis) < 1 or len(scalar) < 1
                if not condition:
                    data_slot = len(data)
                    for d in data:
                        load_date_time = str(d[0]).split(' ')
                        load_date = load_date_time[0]
                        if load_date == str("2022-12-03"):
                            f = open(directory_path + '\\' + folder_name_profile + '\\' + node_id + '.txt', 'a')
                            f.write(str(message.value) + '\n')
                            f.close()
                            slot_dict[node_id] = data_slot
                            slot_count = "Slot_Count"
                            if not os.path.exists(directory_path + '\\' + folder_name_profile + '\\' + slot_count):
                                os.mkdir(directory_path + '\\' + folder_name_profile + '\\' + slot_count)
                            slot_list = open(directory_path + '\\' + folder_name_profile + '\\' + slot_count + "\\"+"slotcount.txt", 'a')
                            slot_list.write(node_id + " : " + str(data_slot) + "\n")
                            slot_list.close()
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
        print("Outside for loop")
        print(slot_dict)
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
        # print(slot_dict)
        print('\nConsumer stopped')
