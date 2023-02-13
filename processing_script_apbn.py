#!/usr/bin/env python
# coding: utf-8

import pandas as pd 
import numpy as np
import joblib
DATA_PATH = '../default_akun.xlsx'
data = pd.read_excel('Normalized_2022_10.xlsx',converters={'AKUN':str})
data = data.rename(columns={'AKUN':'SEGMENT3'})
# filter_akun = data['SEGMENT3'].str.startswith(('4','5','6'))
ref_ = pd.read_excel(DATA_PATH,converters={'KODEAKUN':str})
ref_provinsi = joblib.load('../provinsi.pkl')
filter_akun = data['SEGMENT3'].isin(ref_['KODEAKUN'].unique().tolist())
data = data.loc[filter_akun]



data = data.melt(id_vars=['SEGMENT3'])
data = data.rename(columns={'variable': 'PROVINSI', 'value': 'NILAI'})
data



container = []
for prov in data['PROVINSI'].unique() :
    temp = data.loc[data['PROVINSI']==prov]
    account = set(ref_['KODEAKUN'].unique()).difference(set(temp['SEGMENT3'].tolist()))
    
#     reg = data.loc[data['PROVINSI']==prov,'REGIONAL'].unique().tolist()[0]
    dummy = {'SEGMENT3':list(account),
            'PROVINSI' : [prov for x in range(len(account))], 
            'NILAI' : [0 for x in range(len(account))],
           }
    dummy_data = pd.DataFrame(data=dummy)
    
    temp_df = pd.concat([temp,dummy_data]).drop_duplicates(subset='SEGMENT3')
    print(prov) 
    print(temp.shape)
    print(temp_df.shape)
#     assert temp_df.shape[0]==1686
    container.append(temp_df)
final_data = pd.concat(container)



mapper = { 'NANGGROE ACEH DARUSSALAM':'ACEH',
'SUMATERA UTARA':'SUMATERA UTARA','SUMATERA BARAT':'SUMATERA BARAT',
'RIAU':'RIAU', 'JAMBI':'JAMBI', 'SUMATERA SELATAN':'SUMATERA SELATAN',
'LAMPUNG':'LAMPUNG','BENGKULU':'BENGKULU', 'BANGKA BELITUNG':'KEP. BANGKA BELITUNG', 'BANTEN':'BANTEN',
'DKI JAKARTA':'DKI JAKARTA','JAWA BARAT':'JAWA BARAT', 'JAWA TENGAH':'JAWA TENGAH',
'DAERAH ISTIMEWA JOGJAKARTA':'DI YOGYAKARTA',
'JAWA TIMUR':'JAWA TIMUR', 'KALIMANTAN BARAT':'KALIMANTAN BARAT',
'KALIMANTAN TENGAH':'KALIMANTAN TENGAH','KALIMANTAN SELATAN':'KALIMANTAN SELATAN',
'KALIMANTAN TIMUR':'KALIMANTAN TIMUR', 'BALI':'BALI',
'NUSA TENGGARA BARAT':'NUSA TENGGARA BARAT', 'NUSA TENGGARA TIMUR':'NUSA TENGGARA TIMUR',
'SULAWESI SELATAN':'SULAWESI SELATAN',
'SULAWESI TENGAH':'SULAWESI TENGAH', 'SULAWESI TENGGARA':'SULAWESI TENGGARA',
'GORONTALO':'GORONTALO',
'SULAWESI UTARA':'SULAWESI UTARA', 'MALUKU UTARA':'MALUKU UTARA', 'MALUKU':'MALUKU',
'PAPUA':'PAPUA','KEPULAUAN RIAU':'KEP. RIAU', 'SULAWESI BARAT':'SULAWESI BARAT',
'PAPUA BARAT':'PAPUA BARAT','KALIMANTAN UTARA':'KALIMANTAN UTARA'}



final_data['PROVINSI'] = final_data['PROVINSI'].map(mapper)



final_data['BULAN'] = 10 
final_data['TAHUN'] = 2022
final_data=final_data.rename(columns={'SEGMENT3':'KODEAKUN',
                          'value':'NILAI',
                          })



regional_mapping = {'ACEH': 'SUMATERA', 
                    'BALI':'BALI DAN NUSA TENGGARA',
                    'BANTEN':'JAWA', 
                    'BENGKULU': 'SUMATERA',
                    'DI YOGYAKARTA':'JAWA', 
                    'GORONTALO':'SULAWESI',
                    'JAMBI': 'SUMATERA',
                    'JAWA BARAT':'JAWA',
                    'JAWA TENGAH':'JAWA',
                    'JAWA TIMUR':'JAWA',
                    'DKI JAKARTA':'JAWA',
                    'KALIMANTAN BARAT':'KALIMANTAN',
                    'KALIMANTAN SELATAN':'KALIMANTAN', 
                    'KALIMANTAN TENGAH':'KALIMANTAN',
                    'KALIMANTAN TIMUR':'KALIMANTAN',
                    'KALIMANTAN UTARA':'KALIMANTAN',
                    'KEP. BANGKA BELITUNG': 'SUMATERA',
                    'KEP. RIAU': 'SUMATERA',
                    'LAMPUNG': 'SUMATERA',
                    'MALUKU':'MALUKU DAN PAPUA',
                    'MALUKU UTARA':'MALUKU DAN PAPUA',
                    'NUSA TENGGARA BARAT':'BALI DAN NUSA TENGGARA',
                    'NUSA TENGGARA TIMUR':'BALI DAN NUSA TENGGARA',
                    'PAPUA':'MALUKU DAN PAPUA',
                    'PAPUA BARAT':'MALUKU DAN PAPUA',
                    'RIAU': 'SUMATERA',
                    'SULAWESI BARAT':'SULAWESI',
                    'SULAWESI SELATAN':'SULAWESI',
                    'SULAWESI TENGAH':'SULAWESI',
                    'SULAWESI TENGGARA':'SULAWESI',
                    'SULAWESI UTARA':'SULAWESI',
                   'SUMATERA BARAT': 'SUMATERA',
                    'SUMATERA SELATAN': 'SUMATERA',
                    'SUMATERA UTARA': 'SUMATERA'}



final_data['REGIONAL'] = final_data['PROVINSI'].map(regional_mapping)



final_data['NILAI'] = final_data['NILAI'].fillna(0)




final_data.to_sql('APBN',con=conn, schema=None, if_exists='append', index=False)