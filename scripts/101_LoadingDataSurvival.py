import pandas as pd
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

db = myclient['ceidg']
entries = db['entries']

# TODO Improve Sex feature by take nationality into acount
# TODO Find out what means last letter in 'DaneDodatkowe.KodyPKD' field
# TODO Correct order of converting string to dates, romove duplicated conversion

query_result = entries.aggregate([
    {'$addFields':
         {'RealDateOfTermination':
              {'$cond':
                   [{'$regexMatch':
                         {'input': {'$cond':
                                        [{'$isArray': '$DaneDodatkowe.PodstawaPrawnaWykreslenia.Informacja'},
                                         'some string',
                                         '$DaneDodatkowe.PodstawaPrawnaWykreslenia.Informacja']
                                    },
                          'regex': '^art. 34 ust',
                          'options': 'i'
                          }
                     },
                    {'$dateToString':
                         {'format': "%Y-%m-%d",
                          'date':
                              {'$subtract':
                                   [{'$toDate': '$DaneDodatkowe.DataWykresleniaWpisuZRejestru'},
                                    1000 * 3600 * 24 * 365 * 2]
                               }
                          }
                     },
                    None]
               }

          }
     },
    {'$addFields':
        {'DateOfTerminationOrSuspension':
            {'$ifNull': [
                {'$cond': [{'$gt': ['$DaneDodatkowe.DataWykresleniaWpisuZRejestru', '$RealDateOfTermination']},
                           '$RealDateOfTermination',
                           None]
                 }, '$DaneDodatkowe.DataWykresleniaWpisuZRejestru']
            }
        }
    },
    {'$match': {'DaneDodatkowe.DataRozpoczeciaWykonywaniaDzialalnosciGospodarczej': {'$lte': '2011-01-01'},
                'DanePodstawowe.NIP': {'$ne': None}
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
        'DateOfTermination': 1,
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
          }
     },
    {'$project':
         {'_id': 0,
          'NIP': 1,
          'Status': 1,
          'StartingDateOfTheBusiness': {'$toDate': '$StartingDateOfTheBusiness'},
          'DeletionDateFromTheRegister': 1,
          'DateOfTermination': 1,
          'MainAddressCounty': 1,
          'MainAddressVoivodeship': 1,
          'CorrespondenceAddressCounty': 1,
          'CorrespondenceAddressVoivodeship': 1,
          'MainAndCorrespondenceAreTheSame': 1,
          'NoOfAdditionalPlaceOfTheBusiness': 1,
          'IsPhoneNo': 1,
          'IsEmail': 1,
          'IsWWW': 1,
          'CommunityProperty': 1,
          'HasLicences': 1,
          'NoOfLicences': 1,
          'Sex': 1,
          'HasPolishCitizenship': 1,
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
])

raw_data_surv = pd.DataFrame(list(query_result))

del query_result

raw_data_surv['YearOfStartingOfTheBusiness'] = raw_data_surv.StartingDateOfTheBusiness.dt.strftime('%Y')

raw_data_surv.to_feather('results/raw_data_surv.feather')
