import csv
import json
import os
from datetime import date

ABSOLUTE_PATH = os.path.dirname(__file__)
DATA_DIR_PATH = os.path.join(ABSOLUTE_PATH, 'data')

DATA_DATE_DIRs = os.listdir(DATA_DIR_PATH)

Profile_Files = {
    'Billing_Profile': [],
    'Event_Profile': [],
    'Instant_Profile': [],
    'Load_Profile': [],
    'Midnight_Profile': []
}

today = str(date.today())
today_data_dir = ''
for data_date_dir in DATA_DATE_DIRs:
    if data_date_dir == today:
        today_data_dir = data_date_dir

CURRENT_DATE_PATH = os.path.join(DATA_DIR_PATH, today_data_dir)
current_date_dirs = os.listdir(CURRENT_DATE_PATH)
for cdd in current_date_dirs:
    CURRENT_DATE_PROFILE_PATH = os.path.join(CURRENT_DATE_PATH, cdd)
    current_date_profile_files = os.listdir(CURRENT_DATE_PROFILE_PATH)
    for cdpf in current_date_profile_files:
        Profile_Files[cdd].append(cdpf)

CORRECT_INCORRECT_PATH = os.path.join(ABSOLUTE_PATH, "Correct_Incorrect_Count")
if not os.path.exists(CORRECT_INCORRECT_PATH):
    os.mkdir(CORRECT_INCORRECT_PATH)

correct_node_count = {}
incorrect_node_count = {}


def correct_node():
    return correct_node_count


def incorrect_node():
    return incorrect_node_count


def main():
    for key in Profile_Files.keys():
        CURRENT_DATE_PROFILE_DIR = os.path.join(CURRENT_DATE_PATH, key)
        for file in Profile_Files[key]:
            msg_file = open(CURRENT_DATE_PROFILE_DIR + '\\' + file, 'r')
            for line in msg_file.readlines():
                line = line.replace("\'", "\"")
                message = json.loads(line)
                data_obis = message['dataObis']
                data = message['data']
                scalar_obis = message['scalarObis']
                scalar = message['scalar']
                node_id = str(message['nodeId'])
                KEY = node_id + "_" + key
                condition = len(data_obis) < 1 or len(data) < 1 or len(scalar_obis) < 1 or len(scalar) < 1
                if key == 'Event_Profile':
                    condition = False
                if condition:
                    if KEY in incorrect_node_count.keys():
                        incorrect_count = incorrect_node_count[KEY]
                        incorrect_count = incorrect_count + 1
                        incorrect_node_count[KEY] = incorrect_count
                    else:
                        incorrect_node_count[KEY] = 1
                else:
                    if KEY in correct_node_count.keys():
                        correct_count = correct_node_count[KEY]
                        correct_count = correct_count + 1
                        correct_node_count[KEY] = correct_count
                    else:
                        correct_node_count[KEY] = 1
    CORRECT_INCORRECT_TODAY_PATH = os.path.join(CORRECT_INCORRECT_PATH, today)
    if not os.path.exists(CORRECT_INCORRECT_TODAY_PATH):
        os.mkdir(CORRECT_INCORRECT_TODAY_PATH)

    file = open(CORRECT_INCORRECT_TODAY_PATH + '\\' + 'correct_count' + '.csv', 'w', newline='')
    writer = csv.writer(file)
    writer.writerow(['NodeId_Profile', 'Correct_Count'])
    for cc in correct_node_count.keys():
        writer.writerow([str(cc), str(correct_node_count[cc])])
    file.close()

    file = open(CORRECT_INCORRECT_TODAY_PATH + '\\' + 'incorrect_count' + '.csv', 'w', newline='')
    writer = csv.writer(file)
    writer.writerow(['NodeId_Profile', 'Incorrect_Count'])
    for cc in incorrect_node_count.keys():
        writer.writerow([str(cc), str(incorrect_node_count[cc])])
    file.close()


if __name__ == '__main__':
    NODE_INIT_DIR = os.path.join(ABSOLUTE_PATH, 'node_init')
    node_files = os.listdir(NODE_INIT_DIR)
    NODE_INIT_FILE_COUNT = os.path.join(ABSOLUTE_PATH, 'node_init_count')
    if not os.path.exists(NODE_INIT_FILE_COUNT):
        os.mkdir(NODE_INIT_FILE_COUNT)

    node_init_count_file = open(NODE_INIT_FILE_COUNT + '\\' + 'node_init_count' + '.csv', 'w', newline='')
    node_init_list = []
    writer = csv.writer(node_init_count_file)
    writer.writerow(['NodeId'])
    for node_file in node_files:
        node_file = node_file.replace('.txt', '')
        writer.writerow([node_file])
        node_init_list.append(node_file)
    node_init_count_file.close()
    # for node_file in node_files:
    #     print(node_file)
    print(node_init_list)
    print(len(node_init_list))
    main()
