
from zeep import Client
import xmltodict
import pickle
import config
import random
import json
from datetime import date, timedelta
from data import ceidg_api as api
import os

password = config.password
url = 'https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/NewDataStoreProvider.svc?wsdl'
cl = Client(wsdl=url)

start_date = date(2012, 1, 1)
end_date = date(2018, 11, 1)
delta = timedelta(weeks=10)

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

ceidg_ids = open('ceidg_ids.pickle', 'rb')
ceidg_ids = pickle.load(ceidg_ids)

random.seed(123)
sampled_ids = random.choices(ceidg_ids, k=500)

j = 30000
k = 33000

while k <= 33000:
    api.ask_with_args(config.password, 'UniqueId', ceidg_ids[j:k], path=os.getcwd() + '/downloaded_nips/')
    j += 1000
    k += 1000


with open('downloaded_nips/ceidg_111119_230127.json') as file:
    data = file.read()

obj = json.loads(data)

for i in range(len(obj)):
    obj[i]['_id'] = obj[i]['IdentyfikatorWpisu']
    obj[i].pop('IdentyfikatorWpisu')

with open('downloaded_nips/ceidg_111119_230127.json', 'w') as file:
    json.dump(obj, file, indent=2)
