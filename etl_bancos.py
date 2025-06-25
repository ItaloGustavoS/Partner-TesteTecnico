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

    try:
        # Fazendo a requisição HTTP para a página da Wikipedia
        response_wiki = requests.get(url_wiki)
        response_wiki.raise_for_status()  # Lança um erro para status HTTP ruins (4xx ou 5xx)

        # Lendo as taxas de câmbio diretamente para um DataFrame Pandas
        df_exchange = pd.read_csv(url_exchange_rates)
        print("Taxas de câmbio carregadas com sucesso.")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar as URLs: {e}")
        return

    # Usando BeautifulSoup para parsear o conteúdo HTML da página
    soup = BeautifulSoup(response_wiki.content, "html.parser")

    # Encontrando a tabela correta. A legenda da tabela que queremos é
    # 'By market capitalization'.
    table_title = "By market capitalization"
    table = soup.find("caption", string=lambda t: t and table_title in t).find_parent(
        "table"
    )

    # Convertendo a tabela HTML para um DataFrame do Pandas
    df_banks_raw = pd.read_html(str(table))[0]

    # Selecionando os 5 maiores bancos
    df_top5_banks = df_banks_raw.head(5)

    # Renomeando as colunas para facilitar o acesso
    df_top5_banks.columns = ["Rank", "Bank name", "Market cap (USD billion)"]
