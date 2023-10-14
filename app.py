import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import json
from difflib import SequenceMatcher
from selenium import webdriver
import time
from datetime import date
import mysql.connector

import re


listaJson = []


def buscarDadosOlx(pages=2, regiao="RJ"):
    regiaoBuscar = {"RJ": "rio-de-janeiro-e-regiao"}
    prefix = {"RJ": "estado-rj"}
    for x in range(0, pages):
        url = "https://" + "olx.com.br" + "/imoveis/" +\
            prefix[regiao] + "/" + regiaoBuscar[regiao]
    if x == 0:
        print("somente a primeira página")
    else:
        url = "https://" + "olx.com.br" + "/imoveis/" +\
            prefix[regiao] + "/" + regiaoBuscar[regiao] + "?o=" + str(x)

    PARAMS = {
        "authority": "rj.olx.com.br",
        "method": "GET",
        "path": "/imoveis/estado-rj/rio-de-janeiro-e-regiao",
        "scheme": "https",
        "referer": "https://rj.olx.com.br/imoveis",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
    }

    page = requests.get(url=url, headers=PARAMS)
    soup = BeautifulSoup(page.content, 'lxml')
    itens = soup.find_all("li", {"class": "sc-1mburcf-1 hqJEoJ"})

    for item in itens:
        try:
            nomeImovel = item.findAll("h2")[0].contents[0]
            precoImovel = item.findAll(
                "h3", {"class": "horizontal priceMobile price sc-ifAKCX bytyxL"})[0].contents[0]
            precoImovel = precoImovel.split("R$")[1]
            precoImovel = float(precoImovel.replace(".", ""))
            dataPostagem = item.findAll(
                "p", {"class": "horizontal date sc-ifAKCX iOyFmS"})[0].contents[0]
            urlImovel = item.find("a")["href"]
            urlImagem = item.find("img")["src"]
            # dadosImovel = item.findAll(
            #     "li", {"class": "sc-gVyKpa fxMrnp"})
            # valorSpan = dadosImovelElement.find("span").contents[0]
            # print(valorSpan)
            regiaoImovel = item.findAll(
                "p", {"class": "sc-ifAKCX iOyFmS"})[0].contents[0]
            try:
                regiaoImovelCidade = regiaoImovel.split(",")[0]
            except:
                regiaoImovelCidade = regiaoImovel

            print("nome Imóvel: " + nomeImovel)
            print("Preço Imóvel: " + str(precoImovel))
            print(dataPostagem)
            print(urlImovel)
            print(urlImagem)
            print(regiaoImovel)

            json = {"dataPostagem": dataPostagem,
                    "nomeImovel": nomeImovel,
                    "precoImovel": precoImovel,
                    "regiaoImovel": regiaoImovel,
                    "urlImovel": urlImovel,
                    "urlImagem": urlImagem,
                    }
            listaJson.append(json)
        except:
            print("erro")


buscarDadosOlx(pages=1)
print(listaJson)

# Configurar a conexão com o banco de dados
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="A9$1c3X#kP0!fG7*oN6@bD5%j",
    database="imoveis_db"
)

# Criar a tabela para armazenar os dados dos imóveis
create_table_query = """
CREATE TABLE imoveis (
  id INT AUTO_INCREMENT PRIMARY KEY,
  data_postagem VARCHAR(255),
  nome_imovel VARCHAR(255),
  preco_imovel FLOAT,
  regiao_imovel VARCHAR(255),
  url_imovel VARCHAR(255)
)
"""
cursor = conn.cursor()
cursor.execute(create_table_query)
conn.commit()

# Inserir os dados dos imóveis na tabela
insert_query = """
INSERT INTO imoveis (data_postagem, nome_imovel, preco_imovel, regiao_imovel, url_imovel)
VALUES (%s, %s, %s, %s, %s)
"""
cursor = conn.cursor()
for item in listaJson:
    data_postagem = item["dataPostagem"]
    nome_imovel = item["nomeImovel"]
    preco_imovel = item["precoImovel"]
    regiao_imovel = item["regiaoImovel"]
    url_imovel = item["urlImovel"]
    values = (data_postagem, nome_imovel,
              preco_imovel, regiao_imovel, url_imovel)
    cursor.execute(insert_query, values)
conn.commit()

# Fechar a conexão com o banco de dados
conn.close()
