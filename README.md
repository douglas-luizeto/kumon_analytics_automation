📊 Kumon Analytics Automation

Este projeto automatiza a coleta, ingestão e transformação de dados de performance escolar para uma unidade do Kumon, utilizando a Modern Data Stack.

🏗️ Arquitetura de DadosOrigem (App): 
Streamlit (Python) para cadastro de alunos e lançamentos de folhas.
Ingestão: Os dados são salvos em Google Sheets (atuando como nossa zona de Landing).
Data Warehouse: BigQuery (Google Cloud) acessando as planilhas via External Tables.
Transformação: dbt (Data Build Tool) para limpeza, tipagem e modelagem (Medallion Architecture).

📂 Estrutura do Repositório

/app: Aplicação Streamlit de interface com o usuário.
/transform: Projeto dbt com os modelos SQL e testes.
/scripts: Scripts auxiliares de migração de dados legados.

🚀 Como Executar

1. Aplicação Streamlit

cd app
streamlit run main.py

2. Pipeline de Dados (dbt)

cd transform
dbt run

🛠️ Tecnologias Utilizadas

Python 3.12 (Pandas, Gspread, Streamlit)
dbt-core + dbt-bigquery
Google Cloud Platform (BigQuery & IAM)
Google Sheets API