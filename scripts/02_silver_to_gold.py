import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

bucket_silver = "s3://tech-challenge-aws-data-hackers/silver/"
bucket_gold = "s3://tech-challenge-aws-data-hackers/gold/"

print("Lendo os dados da camada Silver...")
df_2023 = spark.read.parquet(f"{bucket_silver}state_of_data_2023/")
df_2024 = spark.read.parquet(f"{bucket_silver}state_of_data_2024/")
df_2025 = spark.read.parquet(f"{bucket_silver}state_of_data_2025_2026/")

# 2. Dicionários de Mapeamento para o Schema Padrão (Gold)
map_2023 = {
    "id": "id",
    "idade": "idade",
    "genero": "genero",
    "cor_raca_etnia": "cor_raca_etnia",
    "estado_onde_mora": "estado",
    "regiao_onde_mora": "regiao",
    "nivel_de_ensino": "nivel_ensino",
    "cargo_atual": "cargo",
    "nivel": "senioridade",
    "faixa_salarial": "faixa_salarial",
    "quais_das_linguagens_listadas_abaixo_voc_utiliza_no_trabalho": "linguagens_utilizadas",
    "dentre_as_op_es_listadas_qual_sua_cloud_preferida": "cloud_utilizada",
    "ano_pesquisa": "ano_pesquisa"
}

map_2024 = {
    "0_a_token": "id",
    "1_a_idade": "idade",
    "1_b_genero": "genero",
    "1_c_cor_raca_etnia": "cor_raca_etnia",
    "1_i_estado_onde_mora": "estado",
    "1_i_2_regiao_onde_mora": "regiao",
    "1_l_nivel_de_ensino": "nivel_ensino",
    "2_f_cargo_atual": "cargo",
    "2_g_nivel": "senioridade",
    "2_h_faixa_salarial": "faixa_salarial",
    "2_r_modelo_de_trabalho_atual": "modelo_trabalho",
    "4_m_usa_chatgpt_ou_copilot_no_trabalho": "uso_ia_generativa",
    "4_d_linguagem_de_programacao_dia_a_dia": "linguagens_utilizadas",
    "4_h_cloud_dia_a_dia": "cloud_utilizada",
    "ano_pesquisa": "ano_pesquisa"
}

map_2025 = {
    "0_a_token": "id",
    "1_a_idade": "idade",
    "1_b_genero": "genero",
    "1_c_cor_raca_etnia": "cor_raca_etnia",
    "1_i_estado_onde_mora": "estado",
    "1_i_2_regiao_onde_mora": "regiao",
    "1_l_nivel_de_ensino": "nivel_ensino",
    "2_f_cargo_atual": "cargo",
    "2_g_nivel": "senioridade",
    "2_h_faixa_salarial": "faixa_salarial",
    "2_q_modelo_de_trabalho_atual": "modelo_trabalho",
    "4_j_usa_chatgpt_ou_copilot_no_trabalho": "uso_ia_generativa",
    "4_c_linguagem_preferida": "linguagens_utilizadas",
    "4_e_cloud_dia_a_dia": "cloud_utilizada",
    "ano_pesquisa": "ano_pesquisa"
}

# 3. Função para padronizar os DataFrames
def padronizar_dataframe(df, mapping):
    colunas_selecionadas = []
    schema_gold = [
        "id", "idade", "genero", "cor_raca_etnia", "estado", "regiao", 
        "nivel_ensino", "cargo", "senioridade", "faixa_salarial", 
        "modelo_trabalho", "uso_ia_generativa", "linguagens_utilizadas", 
        "cloud_utilizada", "ano_pesquisa"
    ]
    
    for col_gold in schema_gold:
        col_silver = next((k for k, v in mapping.items() if v == col_gold), None)
        if col_silver and col_silver in df.columns:
            colunas_selecionadas.append(col(col_silver).alias(col_gold))
        else:
            colunas_selecionadas.append(lit(None).cast("string").alias(col_gold))
            
    return df.select(*colunas_selecionadas)

print("Padronizando os schemas...")
df_gold_2023 = padronizar_dataframe(df_2023, map_2023)
df_gold_2024 = padronizar_dataframe(df_2024, map_2024)
df_gold_2025 = padronizar_dataframe(df_2025, map_2025)

# 4. Unificando e Salvando
print("Unificando as bases e Salvando na Gold...")
df_analitico = df_gold_2023.unionByName(df_gold_2024).unionByName(df_gold_2025)

df_analitico.write.mode("overwrite").partitionBy("ano_pesquisa").parquet(bucket_gold)
print("Processamento da Camada Gold finalizado com sucesso!")
