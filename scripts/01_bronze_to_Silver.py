import sys
import re
import ast
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, current_timestamp

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

bucket_bronze = "s3://tech-challenge-aws-data-hackers/bronze/"
bucket_silver = "s3://tech-challenge-aws-data-hackers/silver/"

# 2. Limpeza e Tratamento
def clean_column_name(col_name):
    """
    Remove caracteres especiais, acentos e espaços, substituindo por '_' 
    e deixando tudo em minúsculo.
    """
    new_name = re.sub(r'[^a-zA-Z0-9]', '_', str(col_name)).lower()
    new_name = re.sub(r'_+', '_', new_name).strip('_')
    return new_name

def deduplicate_columns(columns):
    """
    Identifica colunas com o mesmo nome e adiciona um sufixo numérico (_1, _2) 
    para evitar o erro COLUMN_ALREADY_EXISTS.
    """
    seen = {}
    new_cols = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    return new_cols

def parse_2023_columns(df):
    """
    As colunas de 2023 vêm como string de tupla (ex: "('P1_b ', 'Genero')").
    Essa função extrai apenas a parte útil (o nome da coluna) usando ast.
    """
    new_columns = []
    for raw_col in df.columns:
        try:
            parsed_tuple = ast.literal_eval(raw_col)
            # Extrai apenas o nome se for tupla, senão usa a string inteira
            if isinstance(parsed_tuple, tuple) and len(parsed_tuple) > 1:
                extracted_name = parsed_tuple[1]
            else:
                extracted_name = raw_col
        except:
            extracted_name = raw_col
        
        new_columns.append(clean_column_name(extracted_name))
    
    # Remove as duplicidades e renomeia o dataframe todo de uma vez
    final_columns = deduplicate_columns(new_columns)
    return df.toDF(*final_columns)

def clean_regular_columns(df):
    """Aplica apenas a limpeza de caracteres nas bases que não são tuplas."""
    new_columns = [clean_column_name(raw_col) for raw_col in df.columns]
    final_columns = deduplicate_columns(new_columns)
    return df.toDF(*final_columns)

# 3. Processamento - Dicionário com as bases
pesquisas = {
    "2023": "state_of_data_2023.csv",
    "2024": "state_of_data_2024.csv",
    "2025_2026": "state_of_data_2025_2026.csv"
}

for ano, arquivo in pesquisas.items():
    print(f"Processando a base do ano: {ano}...")
    
    # Lendo o arquivo CSV da camada Bronze
    df = spark.read.csv(
        f"{bucket_bronze}{arquivo}", 
        header=True, 
        multiLine=True, 
        escape='"',    
        inferSchema=True
    )
    
    # Tratamento específico para as colunas
    if ano == "2023":
        df_limpo = parse_2023_columns(df)
    else:
        df_limpo = clean_regular_columns(df)
    
    # Adicionando colunas de metadados
    df_silver = df_limpo.withColumn("ano_pesquisa", lit(ano)) \
                        .withColumn("data_ingestao_silver", current_timestamp())
    
    # 4. Salvando os dados na camada Silver em formato Parquet
    caminho_saida = f"{bucket_silver}state_of_data_{ano}/"
    df_silver.write.mode("overwrite").parquet(caminho_saida)
    
    print(f"Base de {ano} salva com sucesso em {caminho_saida}!")

print("Processamento da Camada Silver finalizado com sucesso!")
