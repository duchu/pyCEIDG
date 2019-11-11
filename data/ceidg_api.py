from zeep import Client
import xmltodict
import json
import time

# TODO Make documentation of fucntions
# TODO Concern build nested function (ask_ceidg inside ask_ceidg_multiple)


# def ask_ceidg(password, nip):
#
#     client = Client(wsdl='https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/NewDataStoreProvider.svc?wsdl')
#     requested_nip = {'AuthToken': password, 'NIP': str(nip)}
#
#     collected_xml = client.service.GetMigrationDataExtendedAddressInfo(**requested_nip)
#     parsed_dict = xmltodict.parse(collected_xml)
#     return json.dumps(parsed_dict, indent=2)


def ask_ceidg(password, nips):
    # TODO make restriction for number of requested NIPs equals with daily limit establishef by data provider

    client = Client(wsdl='https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/NewDataStoreProvider.svc?wsdl')
    results = {}

    for nip in nips:
        requested_nip = {'AuthToken': password, 'NIP': str(nip)}
        collected_xml = client.service.GetMigrationDataExtendedAddressInfo(**requested_nip)
        parsed_dict = xmltodict.parse(collected_xml)
        results[nip] = parsed_dict['WynikWyszukiwania']['InformacjaOWpisie']
        time.sleep(1)

    return json.dumps(results, indent=2)


def ask_with_args(password, key, values, path):

    # TODO define default arument for 'path' parmeter
    # TODO chage path join
    # TODO Consider make possibility of choice beetween one file and multiple files as output

    client = Client(wsdl='https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/NewDataStoreProvider.svc?wsdl')
    results = {}

    for value in values:
        requested_item = {'AuthToken': password, str(key): value}
        collected_xml = client.service.GetMigrationDataExtendedAddressInfo(**requested_item)
        parsed_dict = xmltodict.parse(collected_xml)
        results[value] = parsed_dict['WynikWyszukiwania']['InformacjaOWpisie']

        nip = results[value]['DanePodstawowe']['NIP']
        with open(path + f'{nip}' + '.json', 'w') as file:
            json.dump(results[value], file)
        time.sleep(1)



