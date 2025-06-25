import requests
import pandas as pd
from bs4 import BeautifulSoup

def run_etl_pipeline():
    """
    Função principal para executar o pipeline de ETL.
    Extrai, transforma e carrega os dados dos maiores bancos.
    """
    
    # --- 1. Extração ---
    # URL da página da Wikipedia com a lista dos maiores bancos
    url_wiki = "https://en.wikipedia.org/wiki/List_of_largest_banks"
    
    # URL do arquivo CSV com as taxas de câmbio
    url_exchange_rates = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
    
    print("Iniciando o processo de extração...")
    
    