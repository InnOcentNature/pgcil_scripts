import csv
import os
import random
from wirepas_mqtt_library import WirepasNetworkInterface
import wirepas_mesh_messaging as wmm
import requests
import logging
import time
from datetime import datetime, date
import re

Parent_dir = "NodeStatus_Prod/"
Instant_dir = "/Instant"
Load_dir = "/Load"
Billing_dir = "/Billing"
Event_dir = "/Event"
Midnight_dir = "/Midnight"
Reboot_dir = "/NodeReboot"
Node_counter_dir = "/Nodecounter"
Node_init_dir = "/NodeInit"
Node_command_status = "/CommandStatus"
Node_command_response = "/CommandResponse"
Server_command = "/ServerCommand"
probus_app = "/ProbusApp"

URL = 'https://rf-adapter-prod.adanielectricity.com:443'
TOKEN = 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9idXMiLCJleHAiOjE5ODA0MTI0NDUsImlhdCI6MTY2NTA1MjQ0NSwiYXV0aG9yaXRpZXMiOltdfQ.c8GRMHOGjdaYHFRq6mM7OK3qEKlwjRto5BjyXPB_zMfNNSdqtivDm5DCQPBy1f8JsrAwbGBFHd78f9rJcqKCiw'

NODE_LIST = [430055]

dcu_node_data = {}


def sendBillingProfile(nodeid):
    randomnum = random.randint(1, 7500)
    print(randomnum)
    dev_test_url = 'https://rf-adapter-prod.adanielectricity.com:443/command/rfCommand'
    # dev_test_token = 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9idXMiLCJleHAiOjE5ODIzOTU0NDYsImlhdCI6MTY2NzAzNTQ0NiwiYXV0aG9yaXRpZXMiOltdfQ.QWBj0DMCWsUKE8DJCjyM8v64D5_5ID1c-_Owx3dKi8phjj5urVcNHlQHxvUg_AFDJIjaQGWZk7rSLock0JBOjQ'
    billingparam = '{"code": "Billing_test","commandDestination": "SENSOR","commandId":4587,"commandType": "P_READ_BILLING","debug": false,"deviceId":' + str(
        nodeid) + ',"properties": [ {"propName": "P_COUNT","propValue": "1"} ] }'
    try:
        header = {"Authorization": TOKEN, "Content-Type": "application/json"}
        response = requests.post(dev_test_url, data=billingparam, headers=header)
        print(response.url)
        print(response)
        if response.status_code == 200:
            res = response.text
            logging.info(res)
            return res
        else:
            logging.info(response)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


def sendMidNightCommand(nodeid):
    randomnum = random.randint(1, 7500)
    print(randomnum)
    dev_test_url = 'https://rf-adapter-prod.adanielectricity.com:443/command/rfCommand'
    # dev_test_token = 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9idXMiLCJleHAiOjE5ODIzOTU0NDYsImlhdCI6MTY2NzAzNTQ0NiwiYXV0aG9yaXRpZXMiOltdfQ.QWBj0DMCWsUKE8DJCjyM8v64D5_5ID1c-_Owx3dKi8phjj5urVcNHlQHxvUg_AFDJIjaQGWZk7rSLock0JBOjQ'
    billingparam = '{"code": "MidNight_Test_08","commandDestination": "SENSOR","commandId":4587,"commandType": "P_READ_MIDNIGHT","debug": false,"deviceId":' + str(
        nodeid) + ',"properties": [ {"propName": "P_FROM","propValue": "1667881284"},{"propName":"P_TO","propValue":"1667967684"} ] }'
    try:
        header = {"Authorization": TOKEN, "Content-Type": "application/json"}
        response = requests.post(dev_test_url, data=billingparam, headers=header)
        print(response.url)
        print(response)
        if response.status_code == 200:
            res = response.text
            logging.info(res)
            return res
        else:
            logging.info(response)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


def sendMeterPassword(gatewayid, sinkid, nodeid):
    #
    m_pass = None
    print(m_pass)
    date_dir = str(date.today())
    dev_test_url = 'https://rf-adapter-prod.adanielectricity.com:443/command/sendMeterPassword'
    if m_pass is None:
        print("nodeid map not found")
        print("gateway node_id#" + gatewayid + " sinkid#" + sinkid + " node node_id#" + str(nodeid))
        with open(Parent_dir + date_dir + Node_init_dir + "/Node_list_not_found.txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + gatewayid + "#" + str(nodeid) + "#" + sinkid
                    + "\n")
        return
    meterpassword = '{"authKey": "", "dedicatedKey": "", "encryptKey": "", "password": "' + m_pass + '","systemTitle": ""}'
    try:
        params = {
            "nodeId": nodeid,
            "gw_id": gatewayid,
            "sink_id": sinkid
        }
        header = {"Authorization": TOKEN, "Content-Type": "application/json"}
        response = requests.post(dev_test_url, params=params, data=meterpassword, headers=header)
        print(response.url)
        print(response)
        if response.status_code == 200:
            res = response.text
            logging.info(res)
            return res
        else:
            logging.info(response)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


def reboot(basic_auth, node_id):
    try:

        url = URL + '/command/restartNode'
        params = {
            'nodeId': node_id
        }
        header = {"Authorization": basic_auth}
        response = requests.post(url, params=params, headers=header)
        print(response.url)
        print(response)

        if response.status_code == 200:

            res = response.text
            logging.info(res)
            return res

        else:
            logging.info(response)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


def on_node_init_rcv(data):
    # print("node init rcv for the nodes")
    try:
        date_dir = str(date.today())
        stringhex = data.data_payload.hex()
        tmp_str = data.data_payload[10:].decode()
        r_f_str = re.sub("[^A-Za-z0-9]", "", tmp_str)
        f_str = str(int(data.source_address)) + ":" + stringhex + ":" + r_f_str
        if data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '0' and data.data_payload.hex()[
            16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[18] == '0' and \
                data.data_payload.hex()[19] == '7':
            try:
                with open(Parent_dir + date_dir + Node_init_dir + "/Node_Init_request_invalid.txt", 'a') as f:
                    f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                        data.destination_endpoint) + "(" + str(
                        len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                        data.travel_time_ms) + "#" + str(data.hop_count) + "#" + f_str + "\n")
            except Exception as e:
                print(e)
        else:
            try:
                with open(Parent_dir + date_dir + Node_init_dir + "/Node_Init_request_valid.txt", 'a') as f:
                    f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                        data.destination_endpoint) + "(" + str(
                        len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                        data.travel_time_ms) + "#" + str(data.hop_count) + "#" + f_str + "\n")
                # print(f_str)
                # print("GOESPHWCNS" in f_str)
                # if "GOESPHWCNS" in f_str:
                #     print("Genus smart meter")
                #     sendMeterPassword(data.gw_id, data.sink_id, data.source_address)
            except Exception as e:
                print(e)

            try:
                with open(Parent_dir + date_dir + Node_init_dir + "/CreateAutoNodeMeterMaster.txt", 'a') as f:
                    f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                        data.destination_endpoint) + "(" + str(
                        len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                        data.travel_time_ms) + "#" + str(data.hop_count) + "#" + r_f_str + "\n")
            except Exception as e:
                print(e)
            # sendMeterPassword(data.gw_id, data.sink_id, data.source_address)
            # m_pass = sensmasterprod.getMeterPassword_prod(str(data.source_address))
            # print(m_pass)
            # if m_pass is None:
            #     print("nodeid map not found")
    except Exception as e:
        print(e)
        with open(Parent_dir + date_dir + Node_init_dir + "/Node_Init_Decode_Failed.txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
        # print(data.data_payload.hex())


def on_node_command_response_rcv(data):
    date_dir = str(date.today())
    try:
        with open(Parent_dir + date_dir + Node_command_response + "/CommandResponse_" + str(data.gw_id) + "_" + str(
                data.source_address) + ".txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "3" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
    except Exception as e:
        print(e)


def writethecontentinfile(path, data):
    try:
        with open(path, 'a') as f:
            f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                int(data.destination_endpoint)) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
    except Exception as e:
        print(e)


def on_node_command_status_rcv(data):
    date_dir = str(date.today())
    # print("command rcvd")
    print(data.source_address)
    if data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == 'c' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'e':
        print("password rejected")
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Node_password_rejected.txt", data)
        # reboot(TOKEN, data.source_address)
    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == 'c' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        print("password accepted")
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Node_password_accept.txt", data)

    elif data.data_payload.hex()[14] == '2' and data.data_payload.hex()[15] == '7' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        print("Enable Accepted")
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Enable_Accepted.txt", data)

    elif data.data_payload.hex()[14] == '2' and data.data_payload.hex()[15] == '7' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'e':
        print("Enable Reject Busy")
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Enable_Rejected_busy.txt", data)
    elif data.data_payload.hex()[14] == '2' and data.data_payload.hex()[15] == '7' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'f':
        print("Enable Rejected")
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Enable_Rejected.txt", data)

    elif data.data_payload.hex()[14] == '2' and data.data_payload.hex()[15] == '8' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'f':
        print("Disable Rejected")
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Disable_Rejected.txt", data)

    elif data.data_payload.hex()[14] == '2' and data.data_payload.hex()[15] == '8' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        print("Disable Accepted")
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Disable_Accepted.txt", data)

    elif data.data_payload.hex()[14] == '2' and data.data_payload.hex()[15] == '8' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'e':
        print("Disable Reject Busy")
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Disable_Rejected_busy.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == 'b' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Time_Sync_Accepted.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == 'b' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'e':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Time_sync_Rejected_busy.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == 'b' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'f':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Time_sync_Rejected.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '1' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Instant_Accepted.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '1' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'f':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Instant_Rejected.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '1' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'e':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Instant_Rejected_busy.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '3' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Billing_Accepted.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '3' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'f':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Billing_Rejected.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '3' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'e':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Billing_Rejected_busy.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '2' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Load_Accepted.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '2' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'f':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Load_Rejected.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '2' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'e':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Load_Rejected_busy.txt", data)
    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '5' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/MidNight_Accepted.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '5' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'f':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Midnight_Rejected.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '5' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'e':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/MidNight_Rejected_busy.txt", data)

    elif data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == 'a' and data.data_payload.hex()[
        16] == '0' and data.data_payload.hex()[17] == '4' and data.data_payload.hex()[24] == 'f' and \
            data.data_payload.hex()[25] == 'd':
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Node_reboot_accept.txt", data)
    else:
        writethecontentinfile(Parent_dir + date_dir + Node_command_status + "/Other_command_status.txt", data)


def on_node_reboot_data_rcv(data):
    date_dir = str(date.today())
    try:
        with open(Parent_dir + date_dir + Reboot_dir + "/Node_reboot.txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
    except Exception as e:
        print(e)


def on_node_meter_data_rcv(data):
    date_dir = str(date.today())
    if data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '1' and data.data_payload.hex()[
        0] == '0' and data.data_payload.hex()[1] == '2':
        try:
            with open(Parent_dir + date_dir + Instant_dir + "/instant_" + str(data.gw_id) + "_" + str(
                    data.source_address) + ".txt", 'a') as f:
                f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                    len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                    data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
        except Exception as e:
            print(e)

    if data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '2' and data.data_payload.hex()[
        0] == '0' and data.data_payload.hex()[1] == '2':
        try:
            with open(Parent_dir + date_dir + Load_dir + "/load_" + str(data.gw_id) + "_" + str(
                    data.source_address) + ".txt", 'a') as f:
                f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                    len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                    data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
        except Exception as e:
            print(e)

    if data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '3' and data.data_payload.hex()[
        0] == '0' and data.data_payload.hex()[1] == '2':
        try:
            with open(Parent_dir + date_dir + Billing_dir + "/billing_" + str(data.gw_id) + "_" + str(
                    data.source_address) + ".txt", 'a') as f:
                f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                    len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                    data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
        except Exception as e:
            print(e)

    if data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '4' and data.data_payload.hex()[
        0] == '0' and data.data_payload.hex()[1] == '2':
        try:
            with open(Parent_dir + date_dir + Event_dir + "/event_" + str(data.gw_id) + "_" + str(
                    data.source_address) + ".txt", 'a') as f:
                f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                    len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                    data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
        except Exception as e:
            print(e)

    if data.data_payload.hex()[14] == '0' and data.data_payload.hex()[15] == '5' and data.data_payload.hex()[
        0] == '0' and data.data_payload.hex()[1] == '2':
        try:
            with open(Parent_dir + date_dir + Midnight_dir + "/midnight_" + str(data.gw_id) + "_" + str(
                    data.source_address) + ".txt", 'a') as f:
                f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                    len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                    data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
        except Exception as e:
            print(e)


def on_node_meter_counters_rcv(data):
    date_dir = str(date.today())
    try:
        with open(Parent_dir + date_dir + Node_counter_dir + "/counters_" + str(data.gw_id) + "_" + str(
                data.source_address) + ".txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "# " + data.data_payload.hex() + "\n")
    except Exception as e:
        print(e)


def on_node_server_command_rcv(data):
    date_dir = str(date.today())
    print(date_dir)
    try:
        with open(Parent_dir + date_dir + Server_command + "/ServerCommands_" + str(data.gw_id) + "_" + str(
                data.source_address) + ".txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
    except Exception as e:
        print(e)


def on_node_diagnostics_data_rcv(data):
    date_dir = str(date.today())
    try:
        with open(Parent_dir + date_dir + Reboot_dir + "/Node_diagnostics.txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
    except Exception as e:
        print(e)


def on_node_traffic_data_rcv(data):
    date_dir = str(date.today())
    try:
        with open(Parent_dir + date_dir + Reboot_dir + "/Node_traffic.txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
    except Exception as e:
        print(e)


def on_node_neighbor(data):
    date_dir = str(date.today())
    try:
        with open(Parent_dir + date_dir + Reboot_dir + "/Node_neighbor.txt", 'a') as f:
            f.write(str((datetime.today())) + "#" + data.gw_id + "#" + str(data.source_address) + "#" + str(
                data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
    except Exception as e:
        print(e)


def probus_app_data(data):
    date_dir = str(date.today())
    if data.source_address in NODE_LIST:
        print(data)
    try:

        with open(Parent_dir + date_dir + probus_app + "/app_" + str(data.gw_id) + "_" + str(
                data.source_address) + ".txt", 'a') as f:

            if str(data.gw_id) in dcu_node_data.keys():

                node_id_set = dcu_node_data[str(data.gw_id)]
                node_id_set.add(str(data.source_address))

                dcu_node_data[str(data.gw_id)] = node_id_set
                # dcu_node_data[str(data.gw_id)+"_"+'count'] = len(node_id_set)

                # print(type(d))
                # print('d' + d)
                # dcu_node_data.update({dcu_node_data[str(data.gw_id)]: d+','+str(data.source_address)})
                # dcu_node_data.update({dcu_node_data[str(data.gw_id)]:node_id_set.add({str(data.source_address)})})
            else:
                dcu_node_data[str(data.gw_id)] = {str(data.source_address)}
                # dcu_node_data[str(data.gw_id) + "_" + 'count']=1
            f.write(str((datetime.today())) + "#" + str(data.destination_endpoint) + "(" + str(
                len(data.data_payload)) + ")#" + data.sink_id + "#" + str(data.destination_address) + "#" + str(
                data.travel_time_ms) + "#" + str(data.hop_count) + "#" + data.data_payload.hex() + "\n")
        if data.destination_endpoint == 63:
            on_node_command_status_rcv(data)
        elif data.destination_endpoint == 61:
            on_node_command_response_rcv(data)
        elif data.destination_endpoint == 60:
            on_node_init_rcv(data)
        elif data.destination_endpoint == 57:
            on_node_meter_data_rcv(data)
        elif data.destination_endpoint == 62:
            on_node_meter_counters_rcv(data)

    except Exception as e:
        print('keyboard in')
        print(e)


def setmqtt():
    # wni = WirepasNetworkInterface('adanigroup.prod-wirepas.com', 8883, 'mqttmasteruser',
    #                               '7Vrs2rLlJNi5uFZv3lk3EF5QzwKGD')
    #
    wni = WirepasNetworkInterface('pgcil-genus-wnt.probussense.com', 8883, 'mqttmasteruser',
                                  'tufWUt2U7uOePi5nuzXOV8OhWV')
    #     register all required call back
    # wni.register_data_cb(on_node_init_rcv, network=256, dst_ep=60)
    # wni.register_data_cb(on_node_command_response_rcv, network=256, dst_ep=61)
    # wni.register_data_cb(on_node_meter_counters_rcv, network=256, dst_ep=62)
    # wni.register_data_cb(on_node_command_status_rcv, network=256, dst_ep=63)
    # wni.register_data_cb(on_node_meter_data_rcv, network=256, dst_ep=57)

    wni.register_data_cb(probus_app_data, network=11220000)
    wni.register_data_cb(probus_app_data, network=11220079)
    wni.register_data_cb(probus_app_data, network=11220122)

    # wni.register_data_cb(on_node_reboot_data_rcv, network=256, dst_ep=255, src_ep=254)
    # wni.register_data_cb(on_node_diagnostics_data_rcv,network=256,dst_ep=255,src_ep=253)
    # wni.register_data_cb(on_node_traffic_data_rcv,network=256,dst_ep=255,src_ep=251)
    # wni.register_data_cb(on_node_neighbor,network=256,dst_ep=255,src_ep=252)

    # wni.register_data_cb(on_node_diagnostic_rcv,network=256,src_ep=253,dst_ep=255)


def check_dirs(node_date):
    # Check whether the specified path exists or not
    isExist = os.path.exists(Parent_dir + str(node_date) + Instant_dir)
    print(Parent_dir + str(node_date) + Instant_dir)
    print(isExist)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(Parent_dir + str(node_date) + Instant_dir)
        print("Instant Dir Created")
    isExist = os.path.exists(Parent_dir + str(node_date) + Billing_dir)
    print(isExist)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(Parent_dir + str(node_date) + Billing_dir)
        print("Billing Dir Created")
    isExist = os.path.exists(Parent_dir + str(node_date) + Load_dir)
    print(isExist)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(Parent_dir + str(node_date) + Load_dir)
        print("Load Dir Created")
    isExist = os.path.exists(Parent_dir + str(node_date) + Event_dir)
    print(isExist)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(Parent_dir + str(node_date) + Event_dir)
        print("Event Dir Created")
    isExist = os.path.exists(Parent_dir + str(node_date) + Midnight_dir)
    print(isExist)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(Parent_dir + str(node_date) + Midnight_dir)
        print("Midnight Dir created")
    isExist = os.path.exists(Parent_dir + str(node_date) + Reboot_dir)
    if not isExist:
        os.mkdir(Parent_dir + str(node_date) + Reboot_dir)
        print("Reboot_dir created")
    isExist = os.path.exists(Parent_dir + str(node_date) + Node_init_dir)
    if not isExist:
        os.mkdir(Parent_dir + str(node_date) + Node_init_dir)
        print("Node_init_dir")
    isExist = os.path.exists(Parent_dir + str(node_date) + Node_counter_dir)
    if not isExist:
        os.mkdir(Parent_dir + str(node_date) + Node_counter_dir)
        print("Node_counter_dir")
    isExist = os.path.exists(Parent_dir + str(node_date) + Node_command_status)
    if not isExist:
        os.mkdir(Parent_dir + str(node_date) + Node_command_status)
        print("node command status")
    isExist = os.path.exists(Parent_dir + str(node_date) + Node_command_response)
    if not isExist:
        os.mkdir(Parent_dir + str(node_date) + Node_command_response)
        print("node command response")
    isExist = os.path.exists(Parent_dir + str(node_date) + Server_command)
    if not isExist:
        os.mkdir(Parent_dir + str(node_date) + Server_command)
        print("server command")
    isExist = os.path.exists(Parent_dir + str(node_date) + probus_app)
    if not isExist:
        os.mkdir(Parent_dir + str(node_date) + probus_app)
        print("server command")


def sendBillingProfile(nodeid):
    randomnum = random.randint(1, 7500)
    print(randomnum)
    dev_test_url = 'https://rf-adapter-prod.adanielectricity.com:443/command/rfCommand'
    # dev_test_token = 'Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9idXMiLCJleHAiOjE5ODIzOTU0NDYsImlhdCI6MTY2NzAzNTQ0NiwiYXV0aG9yaXRpZXMiOltdfQ.QWBj0DMCWsUKE8DJCjyM8v64D5_5ID1c-_Owx3dKi8phjj5urVcNHlQHxvUg_AFDJIjaQGWZk7rSLock0JBOjQ'
    billingparam = '{"code": "Billing_test","commandDestination": "SENSOR","commandId":4587,"commandType": "P_READ_BILLING","debug": false,"deviceId":' + str(
        nodeid) + ',"properties": [ {"propName": "P_COUNT","propValue": "1"} ] }'
    try:
        header = {"Authorization": TOKEN, "Content-Type": "application/json"}
        response = requests.post(dev_test_url, data=billingparam, headers=header)
        print(response.url)
        print(response)
        if response.status_code == 200:
            res = response.text
            logging.info(res)
            return res
        else:
            logging.info(response)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


def read_file_content(path):
    with open(path) as f:
        lines = f.readlines()
    for i in lines:
        print(i.removesuffix('\n'))
        time.sleep(5)
        # sendMidNightCommand(i.removesuffix('\n'))
        # enableAll(i.removesuffix('\n'))


def main():
    print("main program")
    node_date = date.today()
    print(node_date)
    node_date = str(node_date)
    # Parent_dir = "NodeStatus/"+str(node_date)
    check_dirs(node_date)
    setmqtt()
    while True:
        print("old Date:" + node_date)
        print("wait for 10 sec:")
        print("new date:" + str(date.today()))
        if node_date not in str(date.today()):
            print("date is changed ")
            check_dirs(str(date.today()))
            node_date = str(date.today())
        time.sleep(10)


def create_csv_file_dcu_node():
    date_dir = str(date.today())
    with open(Parent_dir + date_dir + probus_app + '/dcu_node.csv', 'w') as csvfile:
        # fieldnames = ['emp_name', 'dept', 'birth_month']
        fieldnames = list(dcu_node_data.keys())
        writer = csv.writer(csvfile)
        for dcu_id in fieldnames:
            writer.writerow([dcu_id, dcu_node_data[dcu_id], len(dcu_node_data[dcu_id])])
        csvfile.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print("main exception")
    finally:
        # print(dcu_node_data)

        create_csv_file_dcu_node()

