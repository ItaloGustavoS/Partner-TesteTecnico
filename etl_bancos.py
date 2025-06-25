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
        # Definindo um User-Agent para simular um navegador
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        # Fazendo a requisição HTTP para a página da Wikipedia com o User-Agent
        response_wiki = requests.get(url_wiki, headers=headers)
        response_wiki.raise_for_status()  # Lança um erro para status HTTP ruins (4xx ou 5xx)

        # Lendo as taxas de câmbio diretamente para um DataFrame Pandas
        df_exchange = pd.read_csv(url_exchange_rates)
        print("Taxas de câmbio carregadas com sucesso.")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar as URLs: {e}")
        return

    # Usando BeautifulSoup para parsear o conteúdo HTML da página
    soup = BeautifulSoup(response_wiki.content, "html.parser")

    # Estratégia alternativa: Encontrar todas as tabelas e identificar a correta por seu conteúdo/cabeçalhos.
    # Estratégia: A tabela "By market capitalization" é geralmente a 3ª tabela com class="wikitable".
    all_wikitables = soup.find_all("table", class_="wikitable")
    # print(f"DEBUG: Found {len(all_wikitables)} tables with class 'wikitable'.") # Debug print removed

    df_banks_raw = None

    if len(all_wikitables) >= 3:
        target_table_html = all_wikitables[2] # 0-indexed, so 2 is the third table
        # print("DEBUG: Assuming the 3rd wikitable is the target for 'By market capitalization'.") # Debug print removed
        try:
            dfs = pd.read_html(str(target_table_html), header=0)
            if dfs:
                df_candidate = dfs[0] # Assume the first DataFrame from this table is the one.
                # print(f"DEBUG: Columns in 3rd wikitable: {df_candidate.columns.tolist()}") # Debug print removed

                if "Bank name" not in df_candidate.columns:
                    pass # print("DEBUG: 'Bank name' column not found in the 3rd wikitable.") # Debug print removed
                else:
                    market_cap_col_candidate = None
                    # Check for specific GlobalData column name (index 2, which is column 3)
                    if len(df_candidate.columns) > 2 and "GlobalData" in str(df_candidate.columns[2]) and ".1" in str(df_candidate.columns[2]):
                        market_cap_col_candidate = df_candidate.columns[2]
                    # Fallback: if not that, check if there's any col at index 2 (3rd column)
                    elif len(df_candidate.columns) > 2:
                        # print(f"DEBUG: Taking column at index 2 as potential market cap: {df_candidate.columns[2]}") # Debug print removed
                        market_cap_col_candidate = df_candidate.columns[2]

                    if market_cap_col_candidate:
                        # print(f"DEBUG: Selected market cap column: '{market_cap_col_candidate}'") # Debug print removed
                        df_banks_raw = df_candidate[["Bank name", market_cap_col_candidate]].copy()
                        df_banks_raw.rename(columns={market_cap_col_candidate: "Market cap (USD billion)"}, inplace=True)
                        # print("Tabela 'By market capitalization' (assumed 3rd wikitable) processed.") # Debug print removed
                    # else:
                        # print("DEBUG: Could not identify a suitable market cap column in the 3rd wikitable.") # Debug print removed
            # else:
                # print("DEBUG: pd.read_html returned no DataFrames for the 3rd wikitable.") # Debug print removed
        except Exception as e:
            print(f"Error processing the assumed 'By market capitalization' table: {e}") # Changed to error for user visibility

    if df_banks_raw is None:
        print(
            "Erro: Não foi possível processar a tabela 'By market capitalization' (assumed 3rd wikitable). O layout da página pode ter mudado."
        )
        sys.exit()

    # Removendo a linha de referência, se existir (começa com '[')
    # e outras linhas que não são nomes de bancos (ex: linhas de cabeçalho repetidas)
    df_banks_raw = df_banks_raw[
        ~df_banks_raw["Bank name"].astype(str).str.startswith("[", na=False) &
        df_banks_raw["Bank name"].notna() &
        ~df_banks_raw["Bank name"].astype(str).str.contains("Rank", na=False, case=False) & # Remover linhas de cabeçalho com "Rank"
        (df_banks_raw["Bank name"].astype(str) != "Bank name") # Remover a linha que é o próprio cabeçalho
    ].copy()

    # Selecionando os 5 maiores bancos
    df_top5_banks = df_banks_raw.head(5).copy()

    # Renomeando as colunas para o padrão que o resto do script espera - já done by rename above
    # df_top5_banks.columns = ["Bank name", "Market cap (USD billion)"] # This line is redundant now

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
