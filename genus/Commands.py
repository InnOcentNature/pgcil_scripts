import datetime
import os
import time
import requests
import logging
import timestamp as timestamp
from get_auth import BASE_URL, auth
from master import genus_master_list

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

token = auth()
cmd_id = "%d" % round(time.time())
today_in_sec = timestamp(datetime.date.today())
week_back_in_sec = timestamp(datetime.date.today() - datetime.timedelta(days=7))
previou_back_in_sec = timestamp(datetime.date.today() - datetime.timedelta(days=1))


def cancelCommands(node_id, cmd_to_cancel):
    try:
        url = BASE_URL + "/command/cancelCommands"
        headers = {"Authorization": token}
        params = {
            'nodeId': node_id,
            'commandId': cmd_id,
            'cancelCommandId': cmd_to_cancel
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)

        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as ERROR:
        logging.error(ERROR)


def changeMeterKey(node_id):
    try:
        url = BASE_URL + "/command/changeMeterKey"
        headers = {"Authorization": token}
        params = {
            'nodeId': node_id,
            'commandId': cmd_id,
            'meterMaker': 'GENUS'
        }
        newMeterKey = {
            "authKey": "",
            "encryptKey": "",
            "masterKey": ""
        }
        response = requests.post(url, json=newMeterKey, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as ERROR:
        logging.error(ERROR)


def connect(node_id):
    try:
        url = BASE_URL + "/command/connectDisconnect"
        headers = {"Authorization": token}
        state = "CONNECTED"
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  'state': state,
                  'mode': 'MODE_NONE',
                  'meterMaker': "GENUS"
                  }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as ERROR:
        logging.error(ERROR)


def disconnect(node_id):
    try:
        url = BASE_URL + "/command/connectDisconnect"
        headers = {"Authorization": token}
        state = "DISCONNECTED"
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  'state': state,
                  'mode': 'MODE_NONE',
                  'meterMaker': "GENUS"
                  }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as ERROR:
        logging.error(ERROR)


def getConnectState(node_id):
    try:
        url = BASE_URL + "/command/getConnectState"
        headers = {"Authorization": token}
        params = {"nodeId": node_id,
                  "commandId": cmd_id,
                  "meterMaker": "GENUS"}
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getBillingDate(node_id):
    try:
        url = BASE_URL + "/command/getBillingDate"
        headers = {"Authorization": token}
        params = {
            'nodeId': node_id,
            'commandId': cmd_id,
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getIntegrationPeriodTime(node_id):
    try:
        url = BASE_URL + "/command/getIntegrationPeriodTime"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getLoadCurtail(node_id):
    try:
        url = BASE_URL + "/command/getLoadCurtail"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id
                  }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getLockOutPeriod(node_id):
    try:
        url = BASE_URL + "/command/getLockOutPeriod"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  'meterMaker': 'GENUS'
                  }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getMeteringMode(node_id):
    try:
        url = BASE_URL + "/command/getMeteringMode"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  'meterMaker': 'GENUS'
                  }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getPrepaidDetails(node_id):
    try:
        url = BASE_URL + "/command/getPrepaidDetails"
        headers = {"Authorization": token}
        params = {
            'nodeId': node_id,
            'commandId': cmd_id,
            'meterMaker': 'GENUS'
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getProfileLogInterval(node_id):
    try:
        url = BASE_URL + "/command/getProfileLogInterval"
        headers = {"Authorization": token}
        params = {
            'nodeId': node_id,
            'commandId': cmd_id
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def listCommands(node_id):
    try:
        url = BASE_URL + "/command/listCommands"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id
                  }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def meterTimeSync(node_id):
    try:
        url = BASE_URL + "/command/meterTimeSync"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  'min': 0,
                  'max': 5
                  }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def restartNode(node_id):
    try:
        url = BASE_URL + "/command/restartNode"
        headers = {"Authorization": token}
        params = {'nodeId': node_id}
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def billing(node_id, hideCommand):
    try:
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token}
        sensorCommand = {
            'code': 'billing_test',
            'commandDestination': "SENSOR",
            'commandId': cmd_id,
            'commandType': 'P_READ_BILLING',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": hideCommand,
            'properties': [
                {
                    'propName': 'P_COUNT',
                    'propValue': "4"
                }]}
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(sensorCommand)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def instant(node_id, hideCommand):
    try:
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token}
        sensorCommand = {
            'code': 'instant_test',
            'commandDestination': "SENSOR",
            'commandId': cmd_id,
            'commandType': 'P_READ_INSTANT',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": hideCommand,
            'properties': []
        }
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def load(node_id, hideCommand):
    try:
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token, "Content-Type": 'application/json'}
        sensorCommand = {
            "code": "load_test",
            "commandDestination": "SENSOR",
            "commandId": cmd_id,
            "commandType": 'P_READ_LOAD',
            "debug": True,
            "deviceId": node_id,
            "hideCommand": hideCommand,
            "properties": [
                {
                    'propName': 'P_FROM',
                    'propValue': str(round(week_back_in_sec / 1000))
                },
                {
                    'propName': 'P_TO',
                    'propValue': str(round(today_in_sec / 1000))
                }
            ]
        }
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def midnight(node_id, hideCommand):
    try:
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token}
        sensorCommand = {
            'code': 'midnight_test',
            'commandDestination': "SENSOR",
            'commandId': cmd_id,
            'commandType': 'P_READ_MIDNIGHT',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": hideCommand,
            'properties': [
                {
                    'propName': 'P_FROM',
                    'propValue': str(round(previou_back_in_sec / 1000))
                },
                {
                    'propName': 'P_TO',
                    'propValue': str(round(today_in_sec / 1000))
                }
            ]
        }
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def rf_command_event_volt(node_id, hideCommand):
    try:
        volt_properties = {
            'P_TYPE': 0,
            'P_COUNT': 1
        }
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token}
        sensorCommand = {
            'code': 'event_test',
            'commandDestination': "SENSOR",
            'commandId': cmd_id,
            'commandType': 'P_READ_EVENT',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": hideCommand,
            'properties': [
                {'propName': 'P_TYPE', 'propValue': volt_properties['P_TYPE']},
                {'propName': 'P_COUNT', 'propValue': volt_properties['P_COUNT']}
            ]}
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def rf_command_event_curr(node_id, hideCommand):
    try:
        curr_properties = {
            'P_TYPE': 1,
            'P_COUNT': 1
        }
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token}
        sensorCommand = {
            'code': 'event_test',
            'commandDestination': "SENSOR",
            'commandId': cmd_id,
            'commandType': 'P_READ_EVENT',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": hideCommand,
            'properties': [
                {'propName': 'P_TYPE', 'propValue': curr_properties['P_TYPE']},
                {'propName': 'P_COUNT', 'propValue': curr_properties['P_COUNT']}
            ]}
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def rf_command_event_power(node_id, hideCommand):
    try:
        power_properties = {
            'P_TYPE': 2,
            'P_COUNT': 1
        }
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token}
        sensorCommand = {
            'code': 'event_test',
            'commandDestination': "SENSOR",
            'commandId': cmd_id,
            'commandType': 'P_READ_EVENT',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": hideCommand,
            'properties': [
                {'propName': 'P_TYPE', 'propValue': power_properties['P_TYPE']},
                {'propName': 'P_COUNT', 'propValue': power_properties['P_COUNT']}
            ]}
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def rf_command_event_tran(node_id, hideCommand):
    try:
        tran_properties = {
            'P_TYPE': 3,
            'P_COUNT': 1
        }
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token}
        sensorCommand = {
            'code': 'event_test',
            'commandDestination': "SENSOR",
            'commandId': cmd_id,
            'commandType': 'P_READ_EVENT',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": hideCommand,
            'properties': [
                {'propName': 'P_TYPE', 'propValue': tran_properties['P_TYPE']},
                {'propName': 'P_COUNT', 'propValue': tran_properties['P_COUNT']}
            ]}
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def rf_command_event_other(node_id, p_type, hideCommand):
    try:
        other_properties = {
            'P_TYPE': p_type,
            'P_COUNT': 1
        }
        url = BASE_URL + "/command/rfCommand"
        headers = {"Authorization": token}
        data = {'code': 'event_test',
                'commandDestination': "SENSOR",
                'commandId': cmd_id,
                'commandType': 'P_READ_EVENT',
                'debug': True,
                'deviceId': node_id,
                "hideCommand": hideCommand,
                'properties': [
                    {'propName': 'P_TYPE', 'propValue': other_properties['P_TYPE']},
                    {'propName': 'P_COUNT', 'propValue': other_properties['P_COUNT']}
                ]}
        response = requests.post(url, json=data, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def sendMeterPassword(node_id, gw_id, sink_id):
    try:
        url = BASE_URL + "/command/sendMeterPassword"
        headers = {"Authorization": token}
        params = {
            'nodeId': node_id,
            'gwId': gw_id,
            'sinkId': sink_id
        }
        meterPassword = {
            "authKey": "",
            "dedicatedKey": "",
            "encryptKey": "",
            "password": 'AeMlHlSugaPl01ab',
            "systemTitle": ""
        }
        response = requests.post(url, json=meterPassword, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def setBillingDate(node_id):
    try:
        url = BASE_URL + "/command/setBillingDate"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  'billingDate': 10,
                  'meterMaker': 'GENUS'}
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def setIntegrationPeriodTime(node_id):
    try:
        url = BASE_URL + "/command/setIntegrationPeriodTime"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  'integrationPeriodTime': 10,
                  'meterMaker': 'GENUS'}
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def setLoadCurtail(node_id):
    try:
        url = BASE_URL + "/command/setLoadCurtail"
        headers = {"Authorization": token}
        params = {'nodeId': node_id,
                  'commandId': cmd_id,
                  'load': 10,
                  'meterMaker': 'GENUS'}
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def setLockOutPeriod(node_id):
    try:
        url = BASE_URL + "/command/setLockOutPeriod"
        headers = {"Authorization": token}
        params = {
            'nodeId': node_id,
            'commandId': cmd_id,
            'autoReconnectTimeInterval': 900,
            'lockoutTime': 1800,
            'attempts': 5,
            'activationTime': int(time.time()),
            'meterMaker': 'GENUS'
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def setMeteringMode(node_id):
    try:
        url = BASE_URL + '/command/setMeteringMode'
        params = {
            "nodeId": node_id,
            "commandId": cmd_id,
            "meteringMode": "NET",
            "meterMaker": "GENUS"
        }
        headers = {"Authorization": token}
        response = requests.post(url=url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def setPrepaidDetails(node_id):
    try:
        url = BASE_URL + '/command/setPrepaidDetails'
        params = {
            "nodeId": node_id,
            "commandId": cmd_id,
            "meterMaker": "GENUS"
        }
        prepaidRequest = {
            "currentBalanceAmount": 0,
            "currentBalanceTime": int(cmd_id),
            "lastTokenRechargeAmount": 0,
            "lastTokenRechargeTime": int(cmd_id) - 432000000,
            "totalAmountAtLastRecharge": 0
        }
        headers = {"Authorization": token}
        response = requests.post(url=url, params=params, json=prepaidRequest, headers=headers)
        logging.info(response.url)

        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def setPrepaidMode(node_id):
    try:
        url = BASE_URL + '/command/setPrepaidMode'
        params = {
            "nodeId": node_id,
            "commandId": cmd_id,
            "prepaid": True,
            "meterMaker": "GENUS"
        }
        headers = {"Authorization": token}
        response = requests.post(url=url, params=params, headers=headers)
        logging.info(response.url)

        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getMeterDetails(node_id):
    try:
        url = BASE_URL + '/command/getMeterDetails'
        params = {
            "nodeId": node_id,
            "commandId": cmd_id,
        }
        headers = {"Authorization": token}
        response = requests.post(url=url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def disableAll(node_id):
    try:
        url = BASE_URL + '/command/disableAll'
        params = {
            "nodeId": node_id,
            "enableIntervalMinute": 0,
            "commandId": cmd_id,
            "broadcast": False,
        }
        headers = {"Authorization": token}
        response = requests.post(url=url, params=params, headers=headers)
        logging.info(response.url)
        logging.info(params)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def enableAll(node_id):
    try:
        url = BASE_URL + '/command/enableAll'
        params = {
            "nodeId": node_id,
            "commandId": cmd_id,
            "broadcast": False,
        }
        headers = {"Authorization": token}
        response = requests.post(url=url, params=params, headers=headers)
        logging.info(response.url)
        logging.info(params)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


if __name__ == '__main__':
    # ABSOLUTE_PATH = os.path.dirname(__file__)
    # MESSAGE_DIR = os.path.join(ABSOLUTE_PATH, "command_response\\Message")
    #
    # today = str(datetime.date.today())
    # date_str = ''
    # for d in os.listdir(MESSAGE_DIR):
    #     if d == today:
    #         date_str = d
    # DATE_FOLDER = os.path.join(MESSAGE_DIR, date_str)
    #
    # file_name = "20_P_READ_LOAD_EXECUTED.txt"
    # excuted_nodes_file = open(DATE_FOLDER + "\\" + file_name, 'r')
    #
    # excuted_nodes = []
    master_nodes = genus_master_list.NODE_LIST_20

    try:
        count = 1
        for node in master_nodes:
            getConnectState(node, False)
            time.sleep(120)
            print(count)
            count = count + 1
        # for line in excuted_nodes_file.readlines():
        #     line = line.replace("\n", "")
        #     excuted_nodes.append(int(line))
        # node_count = 1
        # for master_node in master_nodes:
        #     if master_node in excuted_nodes:
        #         print("Already excuted : ", str(master_node))
        #         print(node_count)
        #     else:
        #         load(master_node, False)
        #         time.sleep(30)
        #         print(node_count)
        #     node_count = node_count + 1
    except Exception as error:
        print(error)
