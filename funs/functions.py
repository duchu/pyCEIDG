
import xmltodict
import json
from datetime import datetime
from zeep import Client
from os import getcwd
from random import random, seed


# TODO Make documentation of fucntions

def _ask_ceidg (password, key, values, path):
    results = []
    requested_item = {'AuthToken': password, str(key): values}
    collected_xml = client.service.GetMigrationDataExtendedAddressInfo(**requested_item)

    if collected_xml == '<WynikWyszukiwania></WynikWyszukiwania>':
        pass
    else:
        parsed_dict = xmltodict.parse(collected_xml)
        result = [parsed_dict['WynikWyszukiwania']['InformacjaOWpisie']][0]
        return result


def get_ceidg_data (password, key, values, path=getcwd()):
    global client
    client = Client(wsdl='https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/NewDataStoreProvider.svc?wsdl')

    if type(values) == str:
        results = _ask_ceidg(password, key, values, path)
    else:
        results = []
        for value in values:
            entry = _ask_ceidg(password, key, value, path)
            results.append(entry)

    now = datetime.now().strftime('%y%m%d_%H%M%S')

    with open(path + '/ceidg_' + f'{now}' + '.json', 'w') as file:
        json.dump(results, file, indent=2)


def random_date (date_start, date_end, set_seed=None):
    seed(set_seed)
    sampled_date = date_start + (date_end - date_start) * random()
    return sampled_date


def month_diff (date_1, date_2):
    return 12 * (date_1.dt.year - date_2.dt.year) + (date_1.dt.month - date_2.dt.month)
