"""
Glue Job: Bronze -> Silver
Unifica as 3 edições da pesquisa State of Data Brasil (2023, 2024, 2025-2026)
em um único dataset padronizado, particionado por ano_pesquisa.

Como usar no AWS Glue:
1. Crie um Glue Job do tipo "Spark script editor" (Python, Glue 4.0+).
2. Ajuste os parâmetros BRONZE_PATH / SILVER_PATH abaixo (ou passe como
   --BRONZE_PATH e --SILVER_PATH nos Job Parameters do Glue Console).
3. IAM Role: use o LabRole do AWS Academy.
4. Os 3 CSVs devem estar no bucket bronze com estes nomes (ajuste se necessário):
   - state_of_data_2023.csv
   - state_of_data_2024.csv
   - state_of_data_2025_2026.csv
"""

import sys
import ast
import re

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Setup Glue / Spark
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"] if "JOB_NAME" in sys.argv else [],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session

# Ajuste estes dois caminhos para o seu bucket
BRONZE_PATH = "s3://SEU-BUCKET/bronze/"
SILVER_PATH = "s3://SEU-BUCKET/silver/state_of_data/"

FILES = {
    2023: BRONZE_PATH + "state_of_data_2023.csv",
    2024: BRONZE_PATH + "state_of_data_2024.csv",
    2025: BRONZE_PATH + "state_of_data_2025_2026.csv",
}

# ---------------------------------------------------------------------------
# Mapeamento de colunas por ano -> schema padronizado
# (levantado inspecionando os cabeçalhos reais dos 3 arquivos)
# ---------------------------------------------------------------------------
STANDARD_SCHEMA = [
    "id", "idade", "faixa_idade", "genero", "cor_raca_etnia", "pcd",
    "estado_onde_mora", "regiao_onde_mora", "nivel_ensino", "cargo_atual",
    "faixa_salarial", "tempo_experiencia_dados", "modelo_trabalho_atual",
]

# 2023: colunas vêm como string de tupla, ex: "('P1_b ', 'Genero')".
# Mapeamos pelo CÓDIGO da pergunta (ex: P1_b), não pelo nome literal da coluna.
MAP_2023_BY_CODE = {
    "P0": "id",
    "P1_a": "idade",
    "P1_a_1": "faixa_idade",
    "P1_b": "genero",
    "P1_c": "cor_raca_etnia",
    "P1_d": "pcd",
    "P1_i": "estado_onde_mora",
    "P1_i_2": "regiao_onde_mora",
    "P1_l": "nivel_ensino",
    "P2_f": "cargo_atual",
    "P2_h": "faixa_salarial",
    "P2_i": "tempo_experiencia_dados",
    # 2023 não tem um campo direto de "modelo de trabalho atual"
}

# 2024 e 2025-2026: colunas já vêm com nome legível, mas com pequenas
# diferenças de código entre os dois anos (ex: 2.r vs 2.q) - por isso
# mapeamos por NOME COMPLETO da coluna em cada ano.
MAP_2024_BY_NAME = {
    "0.a_token": "id",
    "1.a_idade": "idade",
    "1.a.1_faixa_idade": "faixa_idade",
    "1.b_genero": "genero",
    "1.c_cor/raca/etnia": "cor_raca_etnia",
    "1.d_pcd": "pcd",
    "1.i_estado_onde_mora": "estado_onde_mora",
    "1.i.2_regiao_onde_mora": "regiao_onde_mora",
    "1.l_nivel_de_ensino": "nivel_ensino",
    "2.f_cargo_atual": "cargo_atual",
    "2.h_faixa_salarial": "faixa_salarial",
    "2.i_tempo_de_experiencia_em_dados": "tempo_experiencia_dados",
    "2.r_modelo_de_trabalho_atual": "modelo_trabalho_atual",
}

MAP_2025_BY_NAME = {
    "0.a_token": "id",
    "1.a_idade": "idade",
    "1.a.1_faixa_idade": "faixa_idade",
    "1.b_genero": "genero",
    "1.c_cor/raca/etnia": "cor_raca_etnia",
    "1.d_pcd": "pcd",
    "1.i_estado_onde_mora": "estado_onde_mora",
    "1.i.2_regiao_onde_mora": "regiao_onde_mora",
    "1.l_nivel_de_ensino": "nivel_ensino",
    "2.f_cargo_atual": "cargo_atual",
    "2.h_faixa_salarial": "faixa_salarial",
    "2.i_tempo_de_experiencia_em_dados": "tempo_experiencia_dados",
    "2.q_modelo_de_trabalho_atual": "modelo_trabalho_atual",
}


def parse_2023_tuple_column(raw_col: str):
    """
    As colunas de 2023 vêm como string de tupla Python, ex:
    "('P1_b ', 'Genero')" -> ('P1_b', 'Genero')
    Retorna (codigo, nome_legivel) ou (None, raw_col) se não for parseável.
    """
    try:
        code, label = ast.literal_eval(raw_col)
        return code.strip(), label.strip()
    except Exception:
        return None, raw_col


def load_2023(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.csv(path, header=True, multiLine=True, escape='"', inferSchema=False)

    rename_map = {}
    for raw_col in df.columns:
        code, _label = parse_2023_tuple_column(raw_col)
        if code in MAP_2023_BY_CODE:
            rename_map[raw_col] = MAP_2023_BY_CODE[code]

    df_selected = df.select(
        [F.col(f"`{raw}`").alias(std) for raw, std in rename_map.items()]
    )

    # Coluna que não existe em 2023
    df_selected = df_selected.withColumn("modelo_trabalho_atual", F.lit(None).cast("string"))
    df_selected = df_selected.withColumn("ano_pesquisa", F.lit(2023))
    return df_selected.select(*STANDARD_SCHEMA, "ano_pesquisa")


def load_named(spark: SparkSession, path: str, name_map: dict, ano: int) -> DataFrame:
    df = spark.read.csv(path, header=True, multiLine=True, escape='"', inferSchema=False)

    rename_map = {raw: std for raw, std in name_map.items() if raw in df.columns}
    missing = set(name_map.values()) - set(rename_map.values())

    df_selected = df.select(
        [F.col(f"`{raw}`").alias(std) for raw, std in rename_map.items()]
    )

    for col_name in missing:
        df_selected = df_selected.withColumn(col_name, F.lit(None).cast("string"))

    df_selected = df_selected.withColumn("ano_pesquisa", F.lit(ano))
    return df_selected.select(*STANDARD_SCHEMA, "ano_pesquisa")


def main():
    df_2023 = load_2023(spark, FILES[2023])
    df_2024 = load_named(spark, FILES[2024], MAP_2024_BY_NAME, 2024)
    df_2025 = load_named(spark, FILES[2025], MAP_2025_BY_NAME, 2025)

    df_unificado = df_2023.unionByName(df_2024).unionByName(df_2025)

    # --- Limpeza básica ---
    for c in ["genero", "cor_raca_etnia", "regiao_onde_mora", "estado_onde_mora"]:
        df_unificado = df_unificado.withColumn(c, F.trim(F.col(c)))

    # idade como número
    df_unificado = df_unificado.withColumn(
        "idade", F.col("idade").cast("int")
    )

    # remove linhas totalmente vazias (sem id)
    df_unificado = df_unificado.filter(F.col("id").isNotNull())

    df_unificado.write.mode("overwrite").partitionBy("ano_pesquisa").parquet(SILVER_PATH)

    print(f"Total de registros unificados: {df_unificado.count()}")
    df_unificado.groupBy("ano_pesquisa").count().show()


if __name__ == "__main__":
    main()
