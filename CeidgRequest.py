
from zeep import Client
import xmltodict
import pickle
import config
import json
from datetime import date, timedelta
from funs import functions as api
import os

password = config.password
url = 'https://datastore.ceidg.gov.pl/CEIDG.DataStore/services/NewDataStoreProvider.svc?wsdl'
cl = Client(wsdl=url)

# Defining requested time range of data --------------------------------------------------------------------------------

start_date = date(1990, 1, 1)
end_date = date(1990, 12, 31)
delta = timedelta(weeks=4)

results = []

while start_date < end_date:
    req_large_data = {'AuthToken': config.password, 'DateFrom': f'{start_date}', 'DateTo': f'{start_date + delta}'}
    response = cl.service.GetID(**req_large_data)
    res_dict = xmltodict.parse(response)
    res_dict = res_dict['WynikWyszukiwania']['IdentyfikatorWpisu']
    results = results + res_dict
    print('DateFrom: ' f'{start_date}', ', DateTo: ' f'{start_date + delta} passed!')
    start_date += delta


# with open('data/ceidg_ids_1991.pickle', 'wb') as file:
#     file.write(pickle.dumps(results))

ceidg_ids = open('data/ceidg_ids_1994.pickle', 'rb')
ceidg_ids = pickle.load(ceidg_ids)

j = 0
k = 10000

while k <= 60000:
    api.get_ceidg_data(config.password, 'UniqueId', ceidg_ids[60000:len(ceidg_ids)], path=os.getcwd() + '/downloaded_nips/')
    j += 10000
    k += 10000

# Prepraring data for mongoimport --------------------------------------------------------------------------------------

files = os.listdir('downloaded_nips')
files.remove('.DS_Store')
files.remove('tmp')


for file in files:
    with open(os.path.join(os.path.abspath('downloaded_nips'), file)) as f:
        data = f.read()
        obj = json.loads(data)

        for i in range(len(obj)):
            if obj[i] is None:
                pass
            elif 'IdentyfikatorWpisu' not in obj[i].keys():
                pass
            else:
                obj[i]['_id'] = obj[i]['IdentyfikatorWpisu']
                obj[i].pop('IdentyfikatorWpisu')

    with open(os.path.join(os.path.abspath('downloaded_nips/tmp'), file), 'w') as f:
        json.dump(obj, f, indent=2)
