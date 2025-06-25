# Pipeline ETL de Dados Bancários

Este script Python (`etl_bancos.py`) implementa um pipeline de Extração, Transformação e Carregamento (ETL) para obter dados dos maiores bancos do mundo por capitalização de mercado.

## Funcionalidades

1.  **Extração (Extract)**:
    *   Busca dados da tabela "By market capitalization" da página da Wikipedia: [List of largest banks](https://en.wikipedia.org/wiki/List_of_largest_banks).
    *   Obtém taxas de câmbio de um arquivo CSV hospedado externamente.
    *   Seleciona os 5 maiores bancos com base na capitalização de mercado em USD.

2.  **Transformação (Transform)**:
    *   Converte os valores de capitalização de mercado (originalmente em USD) para outras moedas:
        *   Libra Esterlina (GBP)
        *   Euro (EUR)
        *   Rúpia Indiana (INR)
    *   Os valores são arredondados para duas casas decimais.

3.  **Carregamento (Load)**:
    *   Salva os dados transformados em um arquivo CSV chamado `maiores_bancos_por_capitalizacao.csv`.
    *   O arquivo CSV final contém as colunas: "Nome do Banco", "Capitalização em GBP", "Capitalização em EUR", "Capitalização em INR".

## Como Executar o Script

1.  **Pré-requisitos**:
    *   Python 3.x
    *   Bibliotecas Python: `requests`, `pandas`, `beautifulsoup4`, `lxml`.
        Você pode instalá-las usando pip:
        ```bash
        pip install requests pandas beautifulsoup4 lxml
        ```
        Ou, se houver um arquivo `requirements.txt`:
        ```bash
        pip install -r requirements.txt
        ```

2.  **Execução**:
    Execute o script a partir do terminal:
    ```bash
    python etl_bancos.py
    ```

3.  **Saída**:
    *   O script imprimirá informações sobre o progresso do ETL no console.
    *   Ao final, será gerado o arquivo `maiores_bancos_por_capitalizacao.csv` no mesmo diretório do script, contendo os dados processados.

## Estrutura do Script (`etl_bancos.py`)

*   **`run_etl_pipeline()`**: Função principal que orquestra as etapas de ETL.
    *   **Extração**:
        *   Define os URLs para a página da Wikipedia e o arquivo de taxas de câmbio.
        *   Faz requisições HTTP para obter os dados.
        *   Usa `BeautifulSoup` para parsear o HTML da Wikipedia e encontrar a tabela relevante (atualmente, assume-se que é a terceira tabela com a classe `wikitable`).
        *   Usa `pandas` para ler a tabela HTML e o CSV das taxas de câmbio, convertendo-os em DataFrames.
        *   Limpa e seleciona os dados brutos dos bancos.
    *   **Transformação**:
        *   Converte a coluna de capitalização de mercado para tipo numérico.
        *   Aplica as taxas de câmbio para calcular a capitalização nas moedas GBP, EUR e INR.
    *   **Carregamento**:
        *   Seleciona as colunas finais e as renomeia para o português.
        *   Salva o DataFrame resultante em um arquivo CSV.

## Automação e Escalabilidade do Pipeline

### Automação

1.  **Agendamento de Tarefas (Scheduling)**:
    *   **Cron (Linux/macOS)**: Configurar uma tarefa cron para executar o script `etl_bancos.py` em intervalos regulares (ex: diariamente, semanalmente).
    *   **Agendador de Tarefas (Windows)**: Usar o Agendador de Tarefas para configurar execuções periódicas.

2.  **Conteinerização (Containerization)**:
    *   Empacotar o script e suas dependências em um contêiner Docker para a implantação em diferentes sistemas ou na nuvem.
    *   O contêiner pode ser executado por um agendador ou orquestrador.

### Escalabilidade

1.  **Processamento de Dados em Larga Escala**:
    *   **Pandas com Otimizações**: Para volumes de dados que ainda cabem na memória, mas são grandes, otimizar o uso de Pandas (ex: usar tipos de dados eficientes, processamento em chunks) pode ajudar.

2.  **Fontes de Dados e Destinos**:
    *   **Bancos de Dados**: Em vez de CSVs, carregar os dados em um banco de dados relacional (PostgreSQL, MySQL) ou NoSQL (MongoDB), ou em um data warehouse (BigQuery, Redshift, Snowflake). Isso melhora a capacidade de consulta, integridade e gerenciamento dos dados.
    *   **APIs Robustas**: Se a fonte de dados (Wikipedia) se tornar instável, buscar APIs financeiras mais robustas e confiáveis para obter os dados bancários, embora isso possa envolver custos.

3.  **Monitoramento e Logging**:
    *   Implementar logging detalhado em todo o pipeline para rastrear o progresso e identificar erros rapidamente.
    *   Usar ferramentas de monitoramento para observar a performance do pipeline e a qualidade dos dados.

4.  **Qualidade de Dados**:
    *   Adicionar etapas de validação de dados (ex: usando bibliotecas como `Great Expectations`) para garantir que os dados extraídos e transformados atendam a critérios de qualidade definidos (ex: tipos corretos, faixas de valores esperadas, ausência de nulos onde não deveriam existir).

5.  **Arquitetura Baseada em Eventos**:
    *   Para cenários mais dinâmicos, considerar uma arquitetura orientada a eventos, onde o pipeline é acionado por eventos (ex: atualização da página da Wikipedia detectada, chegada de novos arquivos de taxas de câmbio).
