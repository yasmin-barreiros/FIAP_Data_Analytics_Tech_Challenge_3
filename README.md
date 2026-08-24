# Tech Challenge — Big Data & Analytics

Repositório da solução do Tech Challenge.

## Estrutura

- `01_arquitetura/` — arquitetura AWS em Draw.io.
- `02_ingestao_bronze/` — ingestão e organização das bases no S3.
- `03_etl_silver/` — ETL/ELT, limpeza e transformação.
- `04_analise_gold/` — análise executiva com Spark/PySpark e SQL.
- `05_visualizacoes/` — gráficos finais gerados pelo notebook.
- `06_documentacao/` — checklist, matriz de atendimento e guia visual.
- `07_apresentacao/` — roteiro e, posteriormente, o PDF/PowerPoint final.

## Arquitetura lógica

`Data Hackers → S3 Bronze → Glue/Spark → S3 Silver → Glue Catalog → Gold → Athena/Glue Notebook → DataViz → Material Executivo`

## Objetivo

Construir uma solução de Engenharia de Dados e Analytics para analisar as 3 últimas pesquisas disponíveis do State of Data Brasil, utilizando AWS, Data Lake, processamento distribuído, organização em camadas, catalogação, consultas analíticas, DataViz e Storytelling.

## Perguntas de negócio

1. Como está estruturado o mercado brasileiro de Dados?
2. Quais perfis profissionais são mais valorizados pelo mercado?
3. Qual é o cenário de diversidade de gênero nas carreiras de dados?
4. Quais tecnologias apresentam maior adoção?
5. Qual é o índice de adoção de Inteligência Artificial e suas implicações?
6. Existem diferenças relevantes entre regiões, senioridades ou modelos de trabalho?
7. Quais oportunidades e desafios existem para empresas que desejam investir em Dados e IA?

## Entregas

- Arquitetura AWS;
- scripts e notebooks;
- processamento em camadas Bronze, Silver e Gold;
- consultas e análises com Spark/PySpark e SQL;
- gráficos e DataViz;
- material executivo em PDF/PowerPoint, a ser incluído posteriormente.

## Dados

Evite versionar datasets brutos ou arquivos grandes no Git. Mantenha os dados no S3 e documente a origem e o processo de ingestão.
