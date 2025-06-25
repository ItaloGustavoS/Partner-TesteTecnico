import requests
import sys
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
    table = None
    # O título "By market capitalization" está dentro de uma tag <span> com um ID específico.
    span = soup.find("span", id="By_market_capitalization")
    if span:
        # A tabela que queremos é o próximo elemento 'table' depois do título.
        table = span.find_next("table")

    if table is None:
        print(
            "Erro: Não foi possível encontrar a tabela 'By market capitalization'. O layout da página pode ter mudado."
        )
        sys.exit()  # Encerra o script se a tabela não for encontrada.

    # Convertendo a tabela HTML para um DataFrame do Pandas.
    # O 'header=1' diz ao Pandas para usar a segunda linha do cabeçalho (índice 1) como os nomes das colunas,
    # o que simplifica o cabeçalho de múltiplas linhas.
    df_banks_raw = pd.read_html(str(table), header=1)[0]

    # Selecionamos as colunas que nos interessam. Dada a nova estrutura,
    # queremos 'Bank name' e a primeira coluna 'Market cap (US$ billion)'.
    df_banks_raw = df_banks_raw[["Bank name", "Market cap(US$ billion)"]]

    # Removendo a linha de referência, se existir (começa com '[')
    df_banks_raw = df_banks_raw[
        ~df_banks_raw["Bank name"].str.startswith("[", na=False)
    ]

    # Selecionando os 5 maiores bancos
    df_top5_banks = df_banks_raw.head(5).copy()

    # Renomeando as colunas para o padrão que o resto do script espera
    df_top5_banks.columns = ["Bank name", "Market cap (USD billion)"]

    print("Extração dos dados dos 5 maiores bancos concluída.")
    print("Dados extraídos:")
    print(df_top5_banks)

    # --- 2. Transformação (Transform) ---
    print("\nIniciando o processo de transformação...")

    # Obtendo as taxas de câmbio para as moedas desejadas
    rate_gbp = df_exchange[df_exchange["Currency"] == "GBP"]["Rate"].iloc[0]
    rate_eur = df_exchange[df_exchange["Currency"] == "EUR"]["Rate"].iloc[0]
    rate_inr = df_exchange[df_exchange["Currency"] == "INR"]["Rate"].iloc[0]

    print(f"Taxas de câmbio utilizadas: GBP={rate_gbp}, EUR={rate_eur}, INR={rate_inr}")

    # Convertendo a coluna de capitalização de mercado para tipo numérico
    df_top5_banks["Market cap (USD billion)"] = pd.to_numeric(
        df_top5_banks["Market cap (USD billion)"]
    )

    # Calculando a capitalização nas novas moedas
    df_top5_banks["Capitalização em GBP (billion)"] = round(
        df_top5_banks["Market cap (USD billion)"] * rate_gbp, 2
    )
    df_top5_banks["Capitalização em EUR (billion)"] = round(
        df_top5_banks["Market cap (USD billion)"] * rate_eur, 2
    )
    df_top5_banks["Capitalização em INR (billion)"] = round(
        df_top5_banks["Market cap (USD billion)"] * rate_inr, 2
    )

    print("Transformação dos dados concluída.")

    # --- 3. Carregamento (Load) ---
    print("\nIniciando o processo de carregamento...")

    # Selecionando e renomeando as colunas para o arquivo final
    df_final = df_top5_banks[
        [
            "Bank name",
            "Capitalização em GBP (billion)",
            "Capitalização em EUR (billion)",
            "Capitalização em INR (billion)",
        ]
    ]
    df_final.columns = [
        "Nome do Banco",
        "Capitalização em GBP",
        "Capitalização em EUR",
        "Capitalização em INR",
    ]

    # Salvando o DataFrame final em um arquivo CSV
    output_filename = "maiores_bancos_por_capitalizacao.csv"
    df_final.to_csv(output_filename, index=False)

    print(
        f"Processo de ETL concluído! Os dados foram salvos no arquivo '{output_filename}'."
    )
    print("\nResultado Final:")
    print(df_final)


# Executando o pipeline
if __name__ == "__main__":
    run_etl_pipeline()
