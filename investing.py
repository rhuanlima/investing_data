import lxml.html
import requests
from io import StringIO, BytesIO
from bs4 import BeautifulSoup
import json

import time
import datetime
import pandas as pd
import os
from dateutil.relativedelta import *a

def c_date(date):
    date = date.split(', ')[1]
    datel = date.split(' de ')
    if datel[1] == "janeiro":
        m = 1
    elif datel[1] == "fevereiro":
        m = 2
    elif datel[1] == "março":
        m = 3
    elif datel[1] == "abril":
        m = 4
    elif datel[1] == "maio":
        m = 5
    elif datel[1] == "junho":
        m = 6
    elif datel[1] == "julho":
        m = 7
    elif datel[1] == "agosto":
        m = 8
    elif datel[1] == "setembro":
        m = 9
    elif datel[1] == "outubro":
        m = 10
    elif datel[1] == "novembro":
        m = 11
    else:
        m = 12
    return datetime.datetime(int(datel[2]), m, int(datel[0])).strftime('%d/%m/%Y')


def format_num(numero):
    try:
        numero = numero.replace('/', '')
    except:
        None
    try:
        numero = numero.replace('.', '')
    except:
        None
    try:
        numero = numero.replace(',', '.')
    except:
        None
    if numero == '--':
        numero = 0

    try:
        if numero[-1:] == 'K':
            numero = float(numero[:-1])*1000
        elif numero[-1:] == 'M':
            numero = float(numero[:-1])*1000000
        elif numero[-1:] == 'B':
            numero = float(numero[:-1])*1000000000
        elif numero[-1:] == 'T':
            numero = float(numero[:-1])*1000000000000
        else:
            numero = float(numero)
    except:
        numero = float(numero)
    return numero

url = 'https:\\br.investing.com\economic-calendar\Service\getCalendarFilteredData'

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
           'X-Requested-With': 'XMLHttpRequest',
           'Accept': '*/*',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
           'Connection': 'keep-alive',
           'Host': 'br.investing.com',
           'Origin': 'https://br.investing.com',
           'Referer': 'https://br.investing.com/earnings-calendar/',
           'Content-Length': '101',
           'Content-Type': 'application/x-www-form-urlencoded',
           'Sec-Fetch-Mode':'cors',
           'Sec-Fetch-Site': 'same-origin'
           }

base = pd.DataFrame()
inicio = datetime.datetime.now()
fim = datetime.datetime.now().replace(year=2021)

while inicio < fim:
    print(inicio.strftime('%Y-%m-%d'))
    payload = {'country[]': [110, 17, 52, 29, 25, 54, 145, 34, 174, 163, 32, 70, 6, 170, 27, 37, 107, 36, 122, 11, 15, 113, 24, 59, 143, 90, 112, 26, 5, 45, 71, 22, 51, 21, 39, 93, 14, 48, 33, 106, 23, 10, 35, 92, 94, 68, 103, 42, 109, 105, 188, 7, 172, 20, 60, 43, 87, 44, 125, 53, 38, 57, 4, 55, 100, 56, 238, 162, 9, 12, 41, 46, 193, 202, 63, 61, 123, 138, 178, 75],
            'dateFrom': inicio.strftime('%Y-%m-%d'),
                'dateTo': inicio.strftime('%Y-%m-%d'),
                'currentTab':'custom',
                'limit_from':'0'}

    g = requests.post("https://br.investing.com/earnings-calendar/Service/getCalendarFilteredData/", data=payload, headers=headers)
    data_dic = json.loads(g.text)
    df = pd.read_html('<table>'+data_dic['data']+'</table>')
    df = df[0].copy()
    if df.shape[0] > 1:
        col_names = ['dt_divulgacao', 'empresa', 'lpa', 'p_lpa',
                        'receita', 'p_receita', 'capitalizacao', 'c8', 'c9']
        df.columns = col_names
        del df['c8']
        del df['c9']

        df['dt_divulgacao'] = df['dt_divulgacao'].apply(
            lambda x: 0 if pd.isna(x) else c_date(x))
        df['drop_line'] = df['dt_divulgacao'].apply(
            lambda x: True if x != 0 else False)
        i = 0
        for row in df['dt_divulgacao']:

            if row != 0:
                apoio = row
            else:
                df['dt_divulgacao'][i] = apoio
            i = i+1

        df = df[df['drop_line'] == False].copy()
        del df['drop_line']

        df['empresa'] = df['empresa'].apply(lambda x: x[:-1])
        df['ds_empresa'] = df['empresa'].apply(
            lambda x: x.split('(')[0].strip())
        df['cd_empresa'] = df['empresa'].apply(
            lambda x: x.split('(')[1].strip())

        del df['empresa']

        df['lpa'] = df['lpa'].apply(lambda x: 0 if x == '--' else x)

        df['p_lpa'] = df['p_lpa'].apply(lambda x: x.replace('/ ', ''))
        df['p_lpa'] = df['p_lpa'].apply(lambda x: x.replace('.', ''))
        df['p_lpa'] = df['p_lpa'].apply(lambda x: x.replace(',', '.'))
        df['p_lpa'] = df['p_lpa'].apply(lambda x: '0' if x == '--' else x)
        df['p_lpa'] = df['p_lpa'].apply(lambda x: float(x))

        df['receita'] = df['receita'].apply(lambda x: '0' if x == '--' else x)
        df['p_receita'] = df['p_receita'].apply(lambda x: x.replace('/ ', ''))
        df['p_receita'] = df['p_receita'].apply(lambda x: '0' if x == '--' else x)
        df['p_receita'] = df['p_receita'].apply(lambda x: format_num(x))

        df['capitalizacao'] = df['capitalizacao'].apply(lambda x: format_num(x))

        df = df[['cd_empresa', 'ds_empresa', 'dt_divulgacao','lpa','p_lpa','receita','p_receita','capitalizacao']].copy()

        if base.shape[0] > 0:
            base = base.append(df)
            base = base.copy()
        else:
            base = df.copy()

    inicio = inicio + relativedelta(days=1)

base.to_csv('C:/Users/rhuan/Desktop/base_{}.csv'.format(datetime.datetime.now().strftime("%Y%m%d%H%M%S")),index = False,sep=';',decimal =',')
