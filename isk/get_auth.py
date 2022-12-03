import requests
import logging

BASE_URL = 'http://pgcil-iskraemeco.probussense.com:9999'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def auth():
    try:
        credential = {
            "password": "probus@123",
            "userId": "probus"
        }
        url = BASE_URL + '/auth/login'
        response = requests.post(url=url, json=credential)
        logging.info(response.url)
        if response.status_code == 200:
            res_token = response.text
            logging.info(response)
            logging.info(response.text)
            return res_token
        else:
            logging.error(response)
            logging.error(response.text)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


