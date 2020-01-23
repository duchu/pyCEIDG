import xmltodict
import json
from datetime import datetime
from zeep import Client
from os import getcwd


# TODO Make documentation of fucntions

def _ask_ceidg(password, key, values, path):

    results = []
    requested_item = {'AuthToken': password, str(key): values}
    collected_xml = client.service.GetMigrationDataExtendedAddressInfo(**requested_item)

    if collected_xml == '<WynikWyszukiwania></WynikWyszukiwania>':
        pass
    else:
        parsed_dict = xmltodict.parse(collected_xml)
        result = [parsed_dict['WynikWyszukiwania']['InformacjaOWpisie']][0]
        return result


def get_ceidg_data(password, key, values, path=getcwd()):

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
