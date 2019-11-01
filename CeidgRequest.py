
from zeep import Client
from zeep.wsse.username import UsernameToken
import xmltodict
import json
import pickle
import config
from datetime import date, timedelta

password = config.password
url = 'https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/NewDataStoreProvider.svc?wsdl'
cl = Client(wsdl=url)

start_date = date(2018, 7, 2)
end_date = date(2018, 7, 15)
delta = timedelta(days=3)

results = []

while start_date < end_date:
    req_large_data = {'AuthToken': password, 'DateFrom': f'{start_date}', 'DateTo': f'{start_date + delta}'}
    response = cl.service.GetID(**req_large_data)
    res_dict = xmltodict.parse(response)
    res_dict = res_dict['WynikWyszukiwania']['IdentyfikatorWpisu']
    results = results + res_dict
    start_date += delta
    print('DateFrom: ' f'{start_date}', ', DateTo: ' f'{start_date + delta} passed!')

with open('ceidg_ids.pickle', 'wb') as file:
    file.write(pickle.dumps(results))


