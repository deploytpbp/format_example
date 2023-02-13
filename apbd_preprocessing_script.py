

import pandas as pd 
import numpy as np 
import bamboolib as bam 
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd 
from dotenv import load_dotenv
import os 
import re 
import argparse

def initialize_argparse() : 
    parser = argparse.ArgumentParser()
    #adding argument for each method
    parser.add_argument('--path', required=True)
    args = parser.parse_args()
    return args



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


def establish_connection() : 
    USER='root'
    PASS=1234
    HOST ='localhost'
    PORT =3306
    DB='fiskalekonomi'

    conn = create_engine(
    "mysql+pymysql://root:P4ssw0rd!@103.49.239.218:3306/fiskalekonomi",
        pool_recycle=7200,
    pool_pre_ping=True,
).connect()
    return conn
def parse_account(text) :
    match = re.match(r"([0-9]+)([^\w\s])", text, re.I)
    if match:
        items = list(match.groups())
        return items[0]
    else :
        return '-'
def parse_uraian(text) :
    match =re.sub(r'[^\w\s]','',text)
    match = re.sub(r'[0-9]','',match)
    match = match.lstrip()
    return match




def main(path) : 
    data = pd.read_excel(path)
    reserved_col = data[['TAHUN','PERIODE','LEVEL_AKUN','Akun']]
    data = data.drop(['TAHUN','PERIODE','LEVEL_AKUN','Akun'],axis=1)
    data = data.rename(columns = lambda x: x.replace('Provinsi ', ''))
    data = data.rename(columns = lambda x: x.upper())
    data = data.rename(columns={'KEPULAUAN RIAU':'KEP. RIAU',
                            'BANGKA BELITUNG':'KEP. BANGKA BELITUNG'})
    merged_data = pd.concat([reserved_col,data],axis=1)

    container = []
    for year in merged_data['TAHUN'].unique() : 
        for bulan in merged_data['PERIODE'].unique() :  
            filter_1 = (merged_data['TAHUN']==year) & (merged_data['PERIODE']==bulan) & \
            (merged_data['Akun']=='53. Belanja Tidak Terduga')
            filter_2 = (merged_data['TAHUN']==year) & (merged_data['PERIODE']==bulan) & \
            (merged_data['Akun']=='52. Belanja Modal')
            temp_1 = merged_data.loc[filter_1].replace('53. Belanja Tidak Terduga','531. Belanja Tidak Terduga')
            temp_2 = merged_data.loc[filter_2].replace('52. Belanja Modal','523. Belanja Modal')
            container.append(temp_1)
            container.append(temp_2)
    temp_data = pd.concat(container)
    final_data = pd.concat([merged_data,temp_data])    



    final_data = final_data.rename(columns={'PERIODE':'BULAN'})
    final_data['URAIAN']= final_data['Akun'].apply(lambda x : parse_uraian(x))
    final_data['AKUN']= final_data['Akun'].apply(lambda x : parse_account(x))
    final_data = final_data.loc[~(final_data['AKUN'].isin(['-']))]
    final_data = final_data.drop(['Akun','URAIAN','LEVEL_AKUN'],axis=1)
    final_data = final_data.melt(id_vars=['TAHUN', 'BULAN', 'AKUN'])
    final_data
    mask = (final_data['AKUN'].str.len() == 3)
    final_data = final_data.loc[mask]
    final_data = final_data.rename(columns={'variable': 'PROVINSI', 'value': 'NILAI'})
    final_data



    final_data['REGIONAL'] = final_data['PROVINSI'].map(regional_mapping)  
    final_data = final_data.rename(columns={'AKUN':'KODEAKUN'})
    final_data['NILAI']*= 1_000_000_000
    final_data.to_sql('APBD',con=establish_connection(),index=False,if_exists='append')


if __name__ == '__main__' : 
    #run agprse 
    method_args = initialize_argparse()
    path = method_args.path
    main(path=path)