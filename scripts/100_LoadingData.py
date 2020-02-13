import pandas as pd
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

db = myclient['ceidg']
entries = db['entries']

# TODO Improve Sex feature by take nationality into acount
# TODO Find out what means last letter in 'DaneDodatkowe.KodyPKD' field
# TODO remove unused objects in order to clear memory

query_result = entries.aggregate([
    {'$match': {
        '$or': [
            {'DaneDodatkowe.Status': 'Aktywny'},
            {'DaneDodatkowe.DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej': {'$gte': '2017-11-01',
                                                                                  '$lte': '2018-11-01'
                                                                                  }
             },
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
        'DateOfTerminationOrSuspension': {
            '$ifNull': ['$DaneDodatkowe.DataWykresleniaWpisuZRejestru',
                        '$DaneDodatkowe.DataZawieszeniaWykonywaniaDzialalnosciGospodarczej']

        },
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
        'NoOfAdditionalPlaceOfTheBusiness':
            {'$size':
                 {'$ifNull':
                      [{'$cond': [{'$isArray': '$DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci.Adres'},
                                  '$DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci.Adres',
                                  {'$objectToArray': '$DaneAdresowe.AdresyDodatkowychMiejscWykonywaniaDzialalnosci'}]
                        }, []]

                  }
             },
        'IsPhoneNo': {'$cond': [{'$ne': ['$DaneKontaktowe.Telefon', None]}, 1, 0]},
        'IsEmail': {'$cond': [{'$ne': ['$DaneKontaktowe.AdresPocztyElektronicznej', None]}, 1, 0]},
        'IsFax': {'$cond': [{'$ne': ['$DaneKontaktowe.Faks', None]}, 1, 0]},
        'IsWWW': {'$cond': [{'$ne': ['$DaneKontaktowe.AdresStronyInternetowej', None]}, 1, 0]},
        'CommunityProperty':
            {'$cond':
                 [{'$regexMatch':
                       {'input': '$DaneDodatkowe.MalzenskaWspolnoscMajatkowa', 'regex': 'data', 'options': 'i'}
                   },
                  'ustała',
                  '$DaneDodatkowe.MalzenskaWspolnoscMajatkowa']
             },
        'HasLicences': {'$cond': [{'$gt': ['$Uprawnienia', None]}, 1, 0]},
        'NoOfLicences':
            {'$size':
                 {'$ifNull':
                      [{'$cond': [{'$isArray': '$Uprawnienia.Uprawnienie'}, '$Uprawnienia.Uprawnienie',
                                  {'$objectToArray': '$Uprawnienia'}
                                  ]
                        }, []]
                  }
             },
        'Sex': {
            '$cond': [{'$regexMatch': {'input': '$DanePodstawowe.Imie', 'regex': 'a$', 'options': 'i'}}, 'F', 'M']
        },
        'Citizenship': '$DaneAdresowe.PrzedsiebiorcaPosiadaObywatelstwaPanstw',
        'HasPolishCitizenship': {
            '$cond': [{'$regexMatch': {'input': '$DaneAdresowe.PrzedsiebiorcaPosiadaObywatelstwaPanstw',
                                       'regex': 'Polska',
                                       'options': 'i'
                                       }
                       }, 1, 0]
        },
        'NoOfCitizenships': {'$size': {
            '$ifNull': [{'$split': ['$DaneAdresowe.PrzedsiebiorcaPosiadaObywatelstwaPanstw', ', ']}, []]
        }
        },
        'ShareholderInOtherCompanies':
            {'$cond': [{'$ne': ['$SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca', None]}, 1, 0]},
        'KodyPKD': '$DaneDodatkowe.KodyPKD',
        'AllPKDCodes': {'$split': ['$DaneDodatkowe.KodyPKD', ',']},
        'AllPKDDivisions': {
            '$map': {'input': {'$split': ['$DaneDodatkowe.KodyPKD', ',']},
                     'as': 'new',
                     'in': {'$substrCP': ['$$new', 0, 2]}
                     }
        },
        'AllPKDGroups': {
            '$map': {'input': {'$split': ['$DaneDodatkowe.KodyPKD', ',']},
                     'as': 'new',
                     'in': {'$substrCP': ['$$new', 0, 3]}
                     }
        },
        'AllPKDClasses': {
            '$map': {'input': {'$split': ['$DaneDodatkowe.KodyPKD', ',']},
                     'as': 'new',
                     'in': {'$substrCP': ['$$new', 0, 4]}
                     }
        }
    }
    },
    {'$addFields':
        {'AllPKDSections': {
            '$map': {'input': '$AllPKDDivisions',
                     'as': 'new',
                     'in': {'$concat':
                                [{'$cond': [{'$lte': ['$$new', '03']}, 'A', '']},
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '04']},
                                                      {'$lte': ['$$new', '09']}]
                                             },
                                            'B', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '10']},
                                                      {'$lte': ['$$new', '33']}]
                                             },
                                            'C', '']
                                  },
                                 {'$cond': [{'$eq': ['$$new', '35']}, 'D', '']},
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '36']},
                                                      {'$lte': ['$$new', '39']}]
                                             },
                                            'E', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '41']},
                                                      {'$lte': ['$$new', '43']}]
                                             },
                                            'F', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '45']},
                                                      {'$lte': ['$$new', '47']}]
                                             },
                                            'G', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '49']},
                                                      {'$lte': ['$$new', '53']}]
                                             },
                                            'H', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '55']},
                                                      {'$lte': ['$$new', '56']}]
                                             },
                                            'I', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '58']},
                                                      {'$lte': ['$$new', '62']}]
                                             },
                                            'J', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '64']},
                                                      {'$lte': ['$$new', '66']}]
                                             },
                                            'K', '']
                                  },
                                 {'$cond': [{'$eq': ['$$new', '68']}, 'L', '']},
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '69']},
                                                      {'$lte': ['$$new', '75']}]
                                             },
                                            'M', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '77']},
                                                      {'$lte': ['$$new', '82']}]
                                             },
                                            'N', '']
                                  },
                                 {'$cond': [{'$eq': ['$$new', '84']}, 'O', '']},
                                 {'$cond': [{'$eq': ['$$new', '85']}, 'P', '']},
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '86']},
                                                      {'$lte': ['$$new', '88']}]
                                             },
                                            'Q', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '90']},
                                                      {'$lte': ['$$new', '93']}]
                                             },
                                            'R', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '94']},
                                                      {'$lte': ['$$new', '96']}]
                                             },
                                            'S', '']
                                  },
                                 {'$cond': [{'$and': [{'$gte': ['$$new', '97']},
                                                      {'$lte': ['$$new', '98']}]
                                             },
                                            'T', '']
                                  },
                                 {'$cond': [{'$eq': ['$$new', '99']}, 'U', '']},

                                 ]
                            }
                     }
        }
        }
    },
    {'$addFields':
         {'NoOfUniquePKDSections': {'$size': {'$ifNull': [{'$setDifference': ['$AllPKDSections', []]}, []]}},
          'NoOfUniquePKDDivsions': {'$size': {'$ifNull': [{'$setDifference': ['$AllPKDDivisions', []]}, []]}},
          'NoOfUniquePKDGroups': {'$size': {'$ifNull': [{'$setDifference': ['$AllPKDGroups', []]}, []]}},
          'NoOfUniquePKDClasses': {'$size': {'$ifNull': [{'$setDifference': ['$AllPKDClasses', []]}, []]}},
          'PKDMainSection': {'$arrayElemAt': ['$AllPKDSections', 0]},
          'PKDMainDivision': {'$arrayElemAt': ['$AllPKDDivisions', 0]},
          'PKDMainGroup': {'$arrayElemAt': ['$AllPKDGroups', 0]},
          'PKDMainClass': {'$arrayElemAt': ['$AllPKDClasses', 0]},
          'StartDate': {'$toDate': {'$cond':
                                        [{'$lt':
                                              ['$StartingDateOfTheBusiness', '2017-11-01']
                                          }, '2017-11-01', '$StartingDateOfTheBusiness']
                                    }
                        },
          'EndDate': {'$toDate':
              {'$cond':
                  [{'$or': [
                      {'$gt': ['$DateOfTerminationOrSuspension', '2018-11-01']},
                      {'$eq': ['$DateOfTerminationOrSuspension', None]}]
                  },
                      '2018-11-01',
                      '$DateOfTerminationOrSuspension']
              }
          }
          }
     },
    {'$project':
         {'NIP': 1,
          'Status': 1,
          'StartingDateOfTheBusiness': {'$toDate': '$StartingDateOfTheBusiness'},
          ''
          'DateOfTerminationOrSuspension': {'$toDate': '$DateOfTerminationOrSuspension'},
          'StartDate': 1,
          'EndDate': 1,
          'MainAddressCounty': 1,
          'MainAddressVoivodeship': 1,
          'CorrespondenceAddressCounty': 1,
          'CorrespondenceAddressVoivodeship': 1,
          'MainAndCorrespondenceAreTheSame': 1,
          'NoOfAdditionalPlaceOfTheBusiness': 1,
          'IsPhoneNo': 1,
          'IsEmail': 1,
          'IsFax': 1,
          'IsWWW': 1,
          'CommunityProperty': 1,
          'HasLicences': 1,
          'NoOfLicences': 1,
          'Sex': 1,
          'Citizenship': 1,
          'HasPolishCitizenship': 1,
          'NoOfCitizenships': 1,
          'ShareholderInOtherCompanies': 1,
          'PKDMainSection': 1,
          'PKDMainDivision': 1,
          'PKDMainGroup': 1,
          'PKDMainClass': 1,
          'NoOfUniquePKDSections': 1,
          'NoOfUniquePKDDivsions': 1,
          'NoOfUniquePKDGroups': 1,
          'NoOfUniquePKDClasses': 1
          }
     },
    # {'$limit': 100000},

])

query_result = list(query_result)

# comment block --------

db['preprocessed_tmp'].insert_many(query_result)
nip_tmp = db['preprocessed_tmp']

no_of_businesses_query = nip_tmp.aggregate([
    {'$lookup':
         {'from': 'entries',
          'localField': 'NIP',
          'foreignField': 'DanePodstawowe.NIP',
          'as': 'nip_tmp'
          }
     },
    {'$unwind':
         {'path': '$nip_tmp'}
     },
    {'$project':
         {'_id': 1,
          'NIP': 1,
          'nip_tmp._id': 1,
          'PastBusinesses':
              {'$gt': ['$StartingDateOfTheBusiness',
                       {'$toDate': '$nip_tmp.DaneDodatkowe.DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej'}]
               }
          }
     },
    {'$match': {'PastBusinesses': True}},
    {'$group':
         {'_id': {'NIP': '$NIP'},
          'n': {'$sum': 1}
          }
     },
    {'$project':
         {'_id': 0,
          'NIP': '$_id.NIP',
          'NoOfPastBusinesses': '$n'
          }
     }
])

no_of_businesses_results = list(no_of_businesses_query)

preprocessed_data = pd.DataFrame(query_result)
no_of_businesses_data = pd.DataFrame(no_of_businesses_results)

raw_data = pd.merge(preprocessed_data, no_of_businesses_data, how='left', on='NIP')

raw_data.to_pickle('results/raw_data.pickle')

