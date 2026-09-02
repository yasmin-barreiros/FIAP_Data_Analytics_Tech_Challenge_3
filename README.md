# 📊 Tech Challenge: Data Lake & Analytics AWS - State of Data Brazil

## 📌 Sobre o Projeto
Este projeto foi desenvolvido como entrega do **Tech Challenge 3**, com o objetivo de construir uma infraestrutura analítica baseada em nuvem (AWS) para processar, unificar e extrair *insights* executivos das pesquisas **State of Data Brazil** das edições de 2023, 2024 e 2025/2026.

A solução utiliza uma **Arquitetura Medallion** (Bronze, Silver e Gold) 100% *serverless*, focada em escalabilidade e governança de dados, culminando na geração de visualizações via PySpark para apoiar tomadas de decisão estratégicas.

---

## 🏗️ Arquitetura da Solução

<img width="979" height="1079" alt="Diagrama AWS" src="https://github.com/user-attachments/assets/3211bccb-79df-48e2-a69d-eff86bda8f47" />


O fluxo de dados foi desenhado da seguinte forma:
1. **Amazon S3 (Data Lake):** Repositório central de armazenamento dividido logicamente nas camadas Bronze, Silver e Gold.
2. **AWS Glue Jobs (PySpark):** Orquestração e processamento das transformações de dados (ETL).
3. **AWS Glue Crawlers & Data Catalog:** Mapeamento automático dos metadados e inferência de esquemas.
4. **Amazon Athena:** Validação de dados e consultas analíticas (SQL) sobre arquivos Parquet de forma *serverless*.
5. **AWS Glue Notebooks (Jupyter):** Geração de gráficos e análises visuais utilizando as bibliotecas `pandas`, `matplotlib` e `seaborn`.

---

## 🔄 Pipeline de Dados (Arquitetura Medallion)

O pipeline foi construído via scripts PySpark para garantir performance em larga escala:

*   🥉 **Camada Bronze (Raw):** Ingestão dos arquivos brutos originais (`.csv`) armazenados nativamente no S3, preservando o histórico da pesquisa sem alterações.
*   🥈 **Camada Silver (Standardization):** Job PySpark responsável por ler os CSVs, sanitizar os nomes das colunas (remoção de caracteres especiais, espaços e padronização do case) e reescrever os dados no formato colunar otimizado **Parquet**.
*   🥇 **Camada Gold (Analytics):** Job PySpark responsável por:
    *   Mapear os esquemas divergentes entre as 3 edições da pesquisa para um *schema* analítico único.
    *   Realizar a união dos datasets históricos (`unionByName`).
    *   Salvar os dados unificados no formato **Parquet particionado pela coluna `ano_pesquisa`**, minimizando drasticamente os custos e tempo de varredura no Amazon Athena.

---

## 💡 Principais Insights Executivos

A partir da Camada Gold, foram geradas automações em Python para responder as hipóteses de negócio da banca. Destacam-se 4 pilares:

1. **Amadurecimento do Mercado e Senioridade:** O mercado transicionou do ciclo de contratação acelerada de iniciantes para uma fase de maturidade. A entrada de profissionais Júniores encolheu de 27% (2023) para 21% (2025/2026).
2. **Valorização Remuneratória (> R$ 16k):** Embora Engenheiros e Cientistas de Dados tenham maior volume absoluto na faixa de salários altos, as maiores taxas de conversão (densidade proporcional) estão nos cargos de **Arquiteto de Dados (28,6%)** e **Engenheiro de ML/AI (25,5%)**.
3. **Adoção Tecnológica & IA Generativa:** Python e SQL são requisitos mínimos universais. Paralelamente, o uso rotineiro de IA Generativa (ChatGPT, Copilots) atingiu a marca massiva de **98%** dos respondentes, consolidando-se como linha de base operacional.
4. **Mulheres no Mercado de Dados:** A representatividade feminina está estagnada na faixa dos 22%. Evidencia-se um gargalo de diversidade corporativa onde as mulheres representam 28% no nível Júnior, mas a proporção cai continuamente para 20% no nível Especialista/Staff+.

---

## 📂 Estrutura do Repositório

FIAP_Data_Analytics_Tech_Challenge_3/
│
├── architecture/
│   └── diagrama AWS.png  # Diagrama de infraestrutura Cloud
│
├── scripts/
│   ├── 01_bronze_to_silver.py          # ETL (Glue PySpark) - Limpeza e Parquet
│   ├── 02_silver_to_gold.py            # ETL (Glue PySpark) - Schema mapping e unificação
│   └── 03_gold_dataviz_notebook.ipynb  # Jupyter Notebook (Glue) - Geração das análises
│
├── presentation/
│   └── Tech_Challenge_3.pptx  # Apresentação final
│
└── README.md# Tech Challenge 3 — Big Data & Analytics
