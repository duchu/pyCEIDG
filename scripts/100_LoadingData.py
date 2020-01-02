import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

db = myclient['ceidg']
entries = db['entries']

query_result = entries.aggregate(
    [
        {'$match': {
            '$or': [
                {'DaneDodatkowe.Status': 'Aktywny'},
                {'DaneDodatkowe.DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej': {'$gte': '2017-11-01'}},
                {'DaneDodatkowe.DataWykresleniaWpisuZRejestru': {'$gte': '2017-11-01'}},
                {'DaneDodatkowe.DataZawieszeniaWykonywaniaDzialalnosciGospodarczej': {'$gte': '2017-11-01'}}
            ]
        }
        },
        {'$project': {
            '_id': 1,
            'NIP': '$DanePodstawowe.NIP',
            'Status': '$DaneDodatkowe.Status',
            'StartingDateOfTheBusiness': '$DaneDodatkowe.DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej',
            'SuspensionDateOfTheBusiness': '$DaneDodatkowe.DataZawieszeniaWykonywaniaDzialalnosciGospodarczej',
            'ResumptionDateOfTheBusiness': '$DaneDodatkowe.DataWznowieniaWykonywaniaDzialalnosciGospodarczej',
            'TerminationDateOfTheBusiness': '$DaneDodatkowe.DataZaprzestaniaWykonywaniaDzialalnosciGospodarczej',
            'DeletionDateFromTheRegister': '$DaneDodatkowe.DataWykresleniaWpisuZRejestru',
            'MainAddressCounty': {'$toUpper': '$DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.Powiat'},
            'MainAddressVoivodeship': {
                '$toUpper': '$DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.Wojewodztwo'
            },
            'CorrespondenceAddressCounty': {'$toUpper': '$DaneAdresowe.AdresDoDoreczen.Powiat'},
            'CorrespondenceAddressVoivodeship': {'$toUpper': '$DaneAdresowe.AdresDoDoreczen.Wojewodztwo'},
            'MainAndCorrespondenceAreTheSame': {
                '$and':
                    [{'$eq': ['$DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.SIMC',
                              '$DaneAdresowe.AdresDoDoreczen.SIMC']
                      },
                     {'$eq': ['$DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.ULIC',
                              '$DaneAdresowe.AdresDoDoreczen.ULIC']
                      }]
            },
            'IsPhoneNo': {'$cond': [{'$ne': ['$DaneKontaktowe.Telefon', None]}, 1, 0]},
            'IsEmail': {'$cond': [{'$ne': ['$DaneKontaktowe.AdresPocztyElektronicznej', None]}, 1, 0]},
            'IsFax': {'$cond': [{'$ne': ['$DaneKontaktowe.Faks', None]}, 1, 0]},
            'IsWWW': {'$cond': [{'$ne': ['$DaneKontaktowe.AdresStronyInternetowej', None]}, 1, 0]},
            'CommunityProperty': '$DaneDodatkowe.MalzenskaWspolnoscMajatkowa',
            'Sex': {'$cond': [{'$regexMatch': {'input': '$DanePodstawowe.Imie', 'regex': 'a$', 'options': 'i'}}, 'F', 'M']}
        }
        },
        {'$limit': 100}
    ]

)

query_result = list(query_result)

# entries.find(
#     filter={'DaneDodatkowe.Status': 'Aktywny'},
#     projection={'DanePodstawowe.Imie': 1,
#                 'DanePodstawowe.NIP': 1,
#                 'DaneKontaktowe.AdresPocztyElektronicznej': 1,
#                 'DaneKontaktowe.AdresStronyInternetowej': 1,
#                 'DaneKontaktowe.Telefon': 1,
#                 'DaneKontaktowe.Faks': 1,
#                 'DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.Miejscowosc': 1,
#                 'DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.Ulica': 1,
#                 'DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.Budynek': 1,
#                 'DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.KodPocztowy': 1,
#                 'DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.Powiat': 1,
#                 'DaneAdresowe.AdresGlownegoMiejscaWykonywaniaDzialalnosci.Wojewodztwo': 1,
#                 'DaneAdresowe.AdresDoDoreczen.Miejscowosc': 1,
#                 'DaneAdresowe.AdresDoDoreczen.Ulica': 1,
#                 'DaneAdresowe.AdresDoDoreczen.Budynek': 1,
#                 'DaneAdresowe.AdresDoDoreczen.KodPocztowy': 1,
#                 'DaneAdresowe.AdresDoDoreczen.Powiat': 1,
#                 'DaneAdresowe.AdresDoDoreczen.Wojewodztwo': 1,
#                 'DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci.Adres.Miejscowosc': 1,
#                 'DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci.Adres.Ulica': 1,
#                 'DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci.Adres.Budynek': 1,
#                 'DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci.Adres.KodPocztowy': 1,
#                 'DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci.Adres.Powiat': 1,
#                 'DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci.Adres.Wojewodztwo': 1,
#                 'PrzedsiebiorcaPosiadaObywatelstwaPanstw': 1,
#                 'DaneDodatkowe.DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej': 1,
#                 'DaneDodatkowe.DataZawieszeniaWykonywaniaDzialalnosciGospodarczej': 1,
#                 'DaneDodatkowe.DataWznowieniaWykonywaniaDzialalnosciGospodarczej': 1,
#                 'DaneDodatkowe.DataZaprzestaniaWykonywaniaDzialalnosciGospodarczej': 1,
#                 'DaneDodatkowe.DataWykresleniaWpisuZRejestru': 1,
#                 'DaneDodatkowe.MalzenskaWspolnoscMajatkowa': 1,
#                 'DaneDodatkowe.Status': 1,
#                 'DaneDodatkowe.KodyPKD': 1,
#                 'SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca': 1,
#                 'Zakazy': 1,
#
#
#                 }
# ).limit(5)
