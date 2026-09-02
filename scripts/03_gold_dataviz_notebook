import os
import textwrap
from pathlib import Path
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import PercentFormatter

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

PATH_GOLD = "s3://tech-challenge-aws-data-hackers/gold/"
df = spark.read.parquet(PATH_GOLD)
df.createOrReplaceTempView("state_of_data")

# Função para converter Spark DataFrame em Pandas convertendo Decimals para Float
def to_pandas_clean(spark_df):
    df_pd = spark_df.toPandas()
    for col_name in df_pd.columns:
        if df_pd[col_name].dtype == "object":
            df_pd[col_name] = pd.to_numeric(df_pd[col_name], errors="ignore")
    return df_pd

# 2. Configuração de Diretório e Tema
OUTPUT_DIR = Path("graficos_executivos")
OUTPUT_DIR.mkdir(exist_ok=True)

sns.set_theme(style="whitegrid")
PALETTE = {
    "primary": "#3F5E8C", "accent": "#8F4E7A", "accent2": "#4C8C72",
    "neutral": "#B8C0CC", "dark": "#2F3A45", "light": "#E9EDF2",
    "female": "#C66B8E", "male": "#4F81BD", "success": "#5C9A6D"
}

plt.rcParams.update({
    "figure.figsize": (12, 6), "figure.dpi": 120, "savefig.dpi": 220,
    "font.family": "sans-serif", "axes.titleweight": "bold",
    "axes.labelcolor": PALETTE["dark"], "xtick.color": PALETTE["dark"], "ytick.color": PALETTE["dark"]
})

# Funções Auxiliares de Formatação
def fmt_pct(v, dec=1): return f"{v:.{dec}f}%".replace(".", ",")
def fmt_int(v): return f"{int(round(v)):,}".replace(",", ".")
def wrap_label(text, width=34): return "\n".join(textwrap.wrap(str(text), width=width))

def base_axis(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.grid(axis="x", color="#D9DEE7", linewidth=0.8)
    ax.grid(axis="y", visible=False)
    ax.tick_params(axis="y", length=0)
    return ax

def set_exec_title(ax, title, subtitle=None):
    ax.set_title(title, loc="left", fontsize=16, color=PALETTE["dark"], pad=30)
    if subtitle: 
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, ha="left", va="bottom", fontsize=10, color="#667085")

def plot_hbar(data, category, value, title, subtitle, filename, highlight_top=1, color=PALETTE["primary"], value_fmt=fmt_int):
    d = data.copy().sort_values(value, ascending=True)
    labels = d[category].map(lambda x: wrap_label(x, 38))
    colors = [PALETTE["light"]] * len(d)
    for pos in range(min(highlight_top, len(d))): colors[-(pos+1)] = color

    fig, ax = plt.subplots(figsize=(12, max(5.5, len(d)*0.55)))
    ax.barh(labels, d[value], color=colors)
    base_axis(ax)
    set_exec_title(ax, title, subtitle)
    
    xmax = d[value].max() if len(d) else 1
    for i, v in enumerate(d[value]):
        ax.text(v + xmax*0.01, i, f"{value_fmt(v)}", va="center", ha="left", fontsize=9.5, fontweight="bold", color=PALETTE["dark"])
        
    ax.set_xlim(0, xmax * 1.18)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / filename, bbox_inches="tight", facecolor="white")
    plt.show()

def plot_100_stacked(pivot_pct, title, subtitle, filename, colors, legend_title, horizontal=False):
    fig, ax = plt.subplots(figsize=(12, 6))
    if horizontal:
        pivot_pct.plot(kind="barh", stacked=True, ax=ax, color=colors, width=0.72)
        ax.set_xlim(0, 100)
        ax.xaxis.set_major_formatter(PercentFormatter(100))
    else:
        pivot_pct.plot(kind="bar", stacked=True, ax=ax, color=colors, width=0.72)
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_formatter(PercentFormatter(100))
        
    set_exec_title(ax, title, subtitle)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y" if not horizontal else "x", color="#D9DEE7")
    ax.grid(axis="x" if not horizontal else "y", visible=False)
    
    for container in ax.containers:
        labels = [fmt_pct(b.get_width() if horizontal else b.get_height(), 0) if (b.get_width() if horizontal else b.get_height()) >= 6 else "" for b in container]
        ax.bar_label(container, labels=labels, label_type="center", fontsize=9, color="white", fontweight="bold")
        
    ax.legend(title=legend_title, frameon=False, bbox_to_anchor=(1.02,1), loc="upper left")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / filename, bbox_inches="tight", facecolor="white")
    plt.show()

# 3. GERAÇÃO DOS GRÁFICOS PARA ANÁLISE

# 1. Estrutura do Mercado
cargos_2025 = to_pandas_clean(spark.sql("SELECT cargo, COUNT(*) AS total FROM state_of_data WHERE ano_pesquisa LIKE '2025%' AND cargo IS NOT NULL AND TRIM(cargo) != '' GROUP BY cargo ORDER BY total DESC LIMIT 10"))
plot_hbar(cargos_2025, "cargo", "total", "Cargos com Maior Presença no Mercado de Dados", "Top 10 cargos por volume de respondentes | 2025/2026", "01_top_cargos.png", highlight_top=3)

sen_ano = to_pandas_clean(spark.sql("SELECT ano_pesquisa, senioridade, COUNT(*) AS total FROM state_of_data WHERE senioridade IS NOT NULL AND TRIM(senioridade) != '' GROUP BY ano_pesquisa, senioridade"))
pivot_sen = sen_ano.pivot(index="ano_pesquisa", columns="senioridade", values="total").fillna(0)
pivot_sen_pct = pivot_sen.div(pivot_sen.sum(axis=1), axis=0) * 100
plot_100_stacked(pivot_sen_pct[[c for c in ["Júnior","Pleno","Sênior","Especialista/Staff+"] if c in pivot_sen_pct.columns]], "Evolução da Senioridade no Mercado de Dados", "Percentual de profissionais por nível em cada edição", "02_senioridade_evolucao.png", [PALETTE["neutral"], "#7AA6C2", PALETTE["primary"], PALETTE["accent"]], "Senioridade")

# 2. Valorização e Remuneração
sal_vol = to_pandas_clean(spark.sql("SELECT cargo, COUNT(*) AS altas FROM state_of_data WHERE ano_pesquisa LIKE '2025%' AND cargo IS NOT NULL AND (faixa_salarial LIKE '%16.001%' OR LOWER(faixa_salarial) LIKE '%acima de%') GROUP BY cargo ORDER BY altas DESC LIMIT 10"))
plot_hbar(sal_vol, "cargo", "altas", "Volume de Profissionais nas Faixas Salariais Superiores (> R$ 16k)", "Cargos com maior presença absoluta nas faixas de alta remuneração | 2025/2026", "03_remuneracao_volume.png", highlight_top=2, color=PALETTE["accent"])

sal_conc = to_pandas_clean(spark.sql("WITH b AS (SELECT cargo, COUNT(*) as tot, SUM(CASE WHEN faixa_salarial LIKE '%16.001%' OR LOWER(faixa_salarial) LIKE '%acima de%' THEN 1 ELSE 0 END) as alta FROM state_of_data WHERE ano_pesquisa LIKE '2025%' AND cargo IS NOT NULL GROUP BY cargo) SELECT cargo, tot, alta, (100.0 * alta / tot) as pct FROM b WHERE tot >= 10 AND alta > 0 ORDER BY pct DESC LIMIT 10"))
plot_hbar(sal_conc, "cargo", "pct", "Proporção de Profissionais em Faixas Salariais Superiores (> R$ 16k)", "Percentual do próprio cargo em faixas salariais altas (Mínimo: 10 respondentes)", "04_remuneracao_concentracao.png", highlight_top=3, color=PALETTE["accent2"], value_fmt=lambda v: fmt_pct(v,1))

# 3. Diversidade de Gênero
gen_ano = to_pandas_clean(spark.sql("SELECT ano_pesquisa, genero, COUNT(*) AS total FROM state_of_data WHERE genero IN ('Masculino','Feminino') GROUP BY ano_pesquisa, genero"))
pivot_gen = gen_ano.pivot(index="ano_pesquisa", columns="genero", values="total").fillna(0)
pivot_gen_pct = pivot_gen.div(pivot_gen.sum(axis=1), axis=0) * 100
plot_100_stacked(pivot_gen_pct, "Representatividade de Gênero ao Longo dos Anos", "Evolução da proporção de mulheres e homens na área de dados", "05_genero_evolucao.png", [PALETTE["female"], PALETTE["male"]], "Gênero")

gen_sen = to_pandas_clean(spark.sql("SELECT genero, senioridade, COUNT(*) AS total FROM state_of_data WHERE ano_pesquisa LIKE '2025%' AND genero IN ('Masculino','Feminino') AND senioridade IS NOT NULL GROUP BY genero, senioridade"))
pivot_gs = gen_sen.pivot(index="senioridade", columns="genero", values="total").fillna(0)
pivot_gs_pct = pivot_gs.div(pivot_gs.sum(axis=1), axis=0) * 100
plot_100_stacked(pivot_gs_pct, "Diversidade de Gênero por Nível de Senioridade", "Percentual de homens e mulheres em cada nível | 2025/2026", "06_genero_senioridade.png", [PALETTE["female"], PALETTE["male"]], "Gênero", horizontal=True)

# 4. Adoção de Tecnologias
lang_df = df.filter(F.col("linguagens_utilizadas").isNotNull()).withColumn("lang", F.explode(F.split(F.col("linguagens_utilizadas"), ",\\s*"))).withColumn("lang", F.trim(F.col("lang")))
top_lang = to_pandas_clean(lang_df.filter(F.col("ano_pesquisa").like("2025%")).groupBy("lang").count().orderBy(F.desc("count")).limit(10))
plot_hbar(top_lang, "lang", "count", "Linguagens de Programação Mais Utilizadas", "Top 10 linguagens mais citadas | 2025/2026", "07_top_linguagens.png", highlight_top=3, color=PALETTE["accent2"])

cloud_df = df.filter(F.col("cloud_utilizada").isNotNull()).withColumn("cloud", F.explode(F.split(F.col("cloud_utilizada"), ",\\s*"))).withColumn("cloud", F.trim(F.col("cloud")))
top_cloud = to_pandas_clean(cloud_df.filter(F.col("ano_pesquisa").like("2025%")).groupBy("cloud").count().orderBy(F.desc("count")).limit(10))
plot_hbar(top_cloud, "cloud", "count", "Provedores de Cloud Mais Adotados", "Top provedores de nuvem utilizados no dia a dia | 2025/2026", "08_top_cloud.png", highlight_top=3, color=PALETTE["accent"])

# 5. Adoção de Inteligência Artificial Generativa
ia_ano = to_pandas_clean(spark.sql("SELECT ano_pesquisa, CASE WHEN uso_ia_generativa LIKE '%Não utilizo%' THEN 'Não usa' ELSE 'Usa IA' END AS status, COUNT(*) AS total FROM state_of_data WHERE uso_ia_generativa IS NOT NULL GROUP BY ano_pesquisa, CASE WHEN uso_ia_generativa LIKE '%Não utilizo%' THEN 'Não usa' ELSE 'Usa IA' END"))
pivot_ia = ia_ano.pivot(index="ano_pesquisa", columns="status", values="total").fillna(0)
pivot_ia_pct = pivot_ia.div(pivot_ia.sum(axis=1), axis=0) * 100
plot_100_stacked(pivot_ia_pct, "Adoção de IA Generativa no Trabalho por Ano", "Percentual de profissionais utilizando soluções como ChatGPT/Copilot", "09_ia_adocao_ano.png", [PALETTE["neutral"], PALETTE["success"]], "Status IA")

ia_sen = to_pandas_clean(spark.sql("SELECT senioridade, CASE WHEN uso_ia_generativa LIKE '%Não utilizo%' THEN 'Não usa' ELSE 'Usa IA' END AS status, COUNT(*) AS total FROM state_of_data WHERE ano_pesquisa LIKE '2025%' AND senioridade IS NOT NULL AND uso_ia_generativa IS NOT NULL GROUP BY senioridade, CASE WHEN uso_ia_generativa LIKE '%Não utilizo%' THEN 'Não usa' ELSE 'Usa IA' END"))
pivot_ias = ia_sen.pivot(index="senioridade", columns="status", values="total").fillna(0)
pivot_ias_pct = pivot_ias.div(pivot_ias.sum(axis=1), axis=0) * 100
plot_100_stacked(pivot_ias_pct, "Adoção de IA Generativa por Senioridade", "Percentual dentro de cada nível | 2025/2026", "10_ia_senioridade.png", [PALETTE["neutral"], PALETTE["success"]], "Status IA", horizontal=True)

# 6. Regiões e Modelos de Trabalho
reg_ano = to_pandas_clean(spark.sql("SELECT ano_pesquisa, regiao, COUNT(*) AS total FROM state_of_data WHERE regiao IS NOT NULL AND TRIM(regiao) != '' GROUP BY ano_pesquisa, regiao"))
pivot_reg = reg_ano.pivot(index="ano_pesquisa", columns="regiao", values="total").fillna(0)
pivot_reg_pct = pivot_reg.div(pivot_reg.sum(axis=1), axis=0) * 100
plot_100_stacked(pivot_reg_pct, "Distribuição Regional dos Profissionais de Dados", "Participação percentual das regiões por ano", "11_regiao_evolucao.png", sns.color_palette("Blues", n_colors=len(pivot_reg_pct.columns)).as_hex(), "Região")

mod_ano = to_pandas_clean(spark.sql("SELECT ano_pesquisa, modelo_trabalho, COUNT(*) AS total FROM state_of_data WHERE modelo_trabalho IS NOT NULL AND TRIM(modelo_trabalho) != '' GROUP BY ano_pesquisa, modelo_trabalho"))
pivot_mod = mod_ano.pivot(index="ano_pesquisa", columns="modelo_trabalho", values="total").fillna(0)
pivot_mod_pct = pivot_mod.div(pivot_mod.sum(axis=1), axis=0) * 100
plot_100_stacked(pivot_mod_pct, "Evolução do Modelo de Trabalho", "Presencial, Híbrido e Remoto ao longo das edições", "12_modelo_trabalho.png", sns.color_palette("Purples", n_colors=len(pivot_mod_pct.columns)).as_hex(), "Modelo")

# 4. SALVANDO GRÁFICOS NO S3

print("\nEnviando gráficos para o Amazon S3...")
s3_client = boto3.client("s3")
BUCKET_NAME = "tech-challenge-aws-data-hackers"
PREFIX_S3 = "gold/graficos_executivos"

for file_path in OUTPUT_DIR.glob("*.png"):
    s3_key = f"{PREFIX_S3}/{file_path.name}"
    s3_client.upload_file(str(file_path), BUCKET_NAME, s3_key)
    print(f"✓ Enviado: s3://{BUCKET_NAME}/{s3_key}")

print("\nTodas as 12 análises foram geradas e enviadas ao S3 com sucesso!")
