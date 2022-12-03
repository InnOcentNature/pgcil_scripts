import json
import logging
import os
import os.path
import threading
import time
from datetime import date
import csv
from kafka import KafkaConsumer

from master import isk_master_list
from master.isk_master_list import NETWORK

today = date.today()
ABSOLUTE_PATH = os.path.dirname(__file__)
timestamp_status = {}


def get_time_status():
    return timestamp_status


class CommandResponseConsumer(threading.Thread):
    daemon = True

    def run(self):
        print("command-response topic...")
        response_consumer = KafkaConsumer(bootstrap_servers='pgcil-iskraemeco.probussense.com:9092',
                                          auto_offset_reset='earliest',
                                          value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        response_consumer.subscribe(['command-response'])

        RELATIVE_PATH = "command_response"
        RESPONSE_DIRECTORY = os.path.join(ABSOLUTE_PATH, RELATIVE_PATH)
        if not os.path.exists(RESPONSE_DIRECTORY):
            os.mkdir(RESPONSE_DIRECTORY)

        MESSAGE_DIRECTORY = os.path.join(RESPONSE_DIRECTORY, "Message")
        if not os.path.exists(MESSAGE_DIRECTORY):
            os.mkdir(MESSAGE_DIRECTORY)

        TIMESTAMP_STATUS_DIRECTORY = os.path.join(RESPONSE_DIRECTORY, "TimeStampStatus")
        if not os.path.exists(TIMESTAMP_STATUS_DIRECTORY):
            os.mkdir(TIMESTAMP_STATUS_DIRECTORY)

        for message in response_consumer:
            node_id = str(message.value['nodeId'])
            network = ""
            if int(node_id) in isk_master_list.NODE_LIST_20:
                network = NETWORK[0]
            elif int(node_id) in isk_master_list.NODE_LIST_6F:
                network = NETWORK[1]
            elif int(node_id) in isk_master_list.NODE_LIST_9A:
                network = NETWORK[2]
            else:
                network = "other"
            msg_str = str(message.value)
            cmd_id = message.value['commandId']
            command_type = str(message.value['commandType'])
            status = message.value['status']

            message_path = os.path.join(MESSAGE_DIRECTORY, str(today))
            if not os.path.exists(message_path):
                os.mkdir(message_path)

            timestamp_status_path = os.path.join(TIMESTAMP_STATUS_DIRECTORY, str(today))
            if not os.path.exists(timestamp_status_path):
                os.mkdir(timestamp_status_path)

            file_msg = open(message_path + '\\' + "message" + '.txt', 'a')
            file_msg.write(msg_str + "\n")
            file_msg.close()
            print(msg_str)

            if status == "ACCEPTED":
                file_msg = open(message_path + '\\' + network + "_" + command_type + "_" + status + '.txt', 'a')
                file_msg.write(node_id)
                file_msg.write("\n")
                file_msg.close()
            elif status == "EXECUTED":
                file_msg = open(message_path + '\\' + network + "_" + command_type + "_" + status + '.txt', 'a')
                file_msg.write(node_id)
                file_msg.write("\n")
                file_msg.close()
            elif status == "FAILED_AUTH":
                file_msg = open(message_path + '\\' + network + "_" + command_type + "_" + status + '.txt', 'a')
                file_msg.write(node_id)
                file_msg.write("\n")
                file_msg.close()
            elif status == "REJECTED":
                file_msg = open(message_path + '\\' + network + "_" + command_type + "_" + status + '.txt', 'a')
                file_msg.write(node_id)
                file_msg.write("\n")
                file_msg.close()
            elif status == "REJECTED_BUSY":
                file_msg = open(message_path + '\\' + network + "_" + command_type + "_" + status + '.txt', 'a')
                file_msg.write(node_id)
                file_msg.write("\n")
                file_msg.close()
            elif status == "FAILED_NOT_FOUND":
                file_msg = open(message_path + '\\' + network + "_" + command_type + "_" + status + '.txt', 'a')
                file_msg.write(node_id)
                file_msg.write("\n")
                file_msg.close()
            elif status == "FAILED_NO_REPLY":
                file_msg = open(message_path + '\\' + network + "_" + command_type + "_" + status + '.txt', 'a')
                file_msg.write(node_id)
                file_msg.write("\n")
                file_msg.close()
            else:
                file_msg = open(message_path + '\\' + network + "_" + command_type + "_" + status + '.txt', 'a')
                node_id_status = {
                    "nodeId": node_id,
                    "status": status
                }
                file_msg.write(str(node_id_status))
                file_msg.write("\n")
                file_msg.close()

            if node_id + '_' + str(cmd_id) in timestamp_status.keys():
                if message.value['status'] == 'ACCEPTED':
                    timestamp_status[node_id + '_' + str(cmd_id)] = message.timestamp
                if message.value['status'] == 'EXECUTED':
                    temp_v = timestamp_status[node_id + '_' + str(cmd_id)]
                    timestamp_status[node_id + '_' + str(cmd_id)] = str(temp_v) + '||' + str(message.timestamp)
            else:
                if message.value['status'] == 'ACCEPTED':
                    timestamp_status[node_id + '_' + str(cmd_id)] = message.timestamp

            file_time = open(timestamp_status_path + '\\' + 'command_status' + '.csv', 'a', newline='')
            writer = csv.writer(file_time)
            writer.writerow([node_id, cmd_id, message.value['commandType'], message.value['status'], message.timestamp])
            file_time.close()


def main():
    threads = [
        CommandResponseConsumer()
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
        print('\nCommand Response stopped')