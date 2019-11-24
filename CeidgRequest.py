
from zeep import Client
import xmltodict
import pickle
import config
import json
from datetime import date, timedelta
from data import ceidg_api as api
import os

password = config.password
url = 'https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/NewDataStoreProvider.svc?wsdl'
cl = Client(wsdl=url)

# Defining requested time range of data --------------------------------------------------------------------------------

start_date = date(2012, 6, 1)
end_date = date(2018, 11, 1)
delta = timedelta(weeks=10)

results = []

while start_date < end_date:
    req_large_data = {'AuthToken': config.password, 'DateFrom': f'{start_date}', 'DateTo': f'{start_date + delta}'}
    response = cl.service.GetID(**req_large_data)
    res_dict = xmltodict.parse(response)
    res_dict = res_dict['WynikWyszukiwania']['IdentyfikatorWpisu']
    results = results + res_dict
    start_date += delta
    print('DateFrom: ' f'{start_date}', ', DateTo: ' f'{start_date + delta} passed!')

# with open('ceidg_ids.pickle', 'wb') as file:
#     file.write(pickle.dumps(results))

ceidg_ids = open('ceidg_ids.pickle', 'rb')
ceidg_ids = pickle.load(ceidg_ids)

j = 1000
k = 3000

while k <= 20000:
    api.ask_with_args(config.password, 'UniqueId', ceidg_ids[j:k], path=os.getcwd() + '/downloaded_nips/')
    j += 2000
    k += 2000

# Prepraring data for mongoimport --------------------------------------------------------------------------------------

files = os.listdir('downloaded_nips')
files.remove('.DS_Store')

for file in files:
    with open(os.path.join(os.path.abspath('downloaded_nips'), file)) as f:
        data = f.read()
        obj = json.loads(data)

        for i in range(len(obj)):
            obj[i]['_id'] = obj[i]['IdentyfikatorWpisu']
            obj[i].pop('IdentyfikatorWpisu')

    with open(os.path.join(os.path.abspath('downloaded_nips'), file), 'w') as f:
        json.dump(obj, f, indent=2)
