#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from delta import *
from delta.tables import DeltaTable


# Se você precisar adicionar ou alterar partições, considere métodos como merge ou outras operações.


builder = pyspark.sql.SparkSession.builder.appName("Teste Prático")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\

spark = configure_spark_with_delta_pip(builder).getOrCreate()
    


# In[2]:


# Carregar o arquivo CSV
df = spark.read.csv('/home/jovyan/work/financial_data.csv', header=True, inferSchema=True)
df.show()


# In[3]:


from pyspark.sql.functions import col

# Converter a coluna ‘valor’ para o tipo de dados double
df = df.withColumn("valor", col("valor").cast("double"))
df.printSchema()


# In[4]:


# Filtrar as transações com valor acima de 1000
df_filtered = df.filter(col("valor") > 1000)
df_filtered.show()


# In[5]:


# Salvar os dados filtrados em um Delta Lake, particionados pela data da transação
df_filtered.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("data_transacao") \
    .save("/home/jovyan/work/delta/financial_data")


# In[7]:


from delta.tables import DeltaTable

# Especificando o caminho onde os dados Delta estão armazenados
delta_path = "/home/jovyan/work/delta/financial_data"

# Criando a Delta Table a partir dos dados salvos
spark.sql(f"""
    CREATE TABLE financial_data
    USING DELTA
    LOCATION '{delta_path}'
""")


# In[9]:


# Realizar uma consulta para selecionar todas as transações com valor acima de 2000
df_high_value = spark.sql("SELECT * FROM financial_data WHERE valor > 2000")
df_high_value.show()


# In[10]:


# Exibir o histórico de versões da Delta Table
delta_table = DeltaTable.forPath(spark, "/home/jovyan/work/delta/financial_data")
delta_table.history().show()


# In[14]:


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Definindo o esquema dos novos dados
schema = StructType([
    StructField("id_transacao", IntegerType(), True),
    StructField("data_transacao", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("tipo_transacao", StringType(), True),
    StructField("descricao", StringType(), True)
])

# Criando um DataFrame com os novos dados
new_data = [
    (11, '2024-07-08', 3000.95, 'DEPOSITO', 'Bônus Anual'),
    (12, '2024-07-08', -150.40, 'PAGAMENTO', 'Restaurante'),
    (13, '2024-07-09', 800.75, 'TRANSFERENCIA', 'Transferência da Conta Corrente'),
    (14, '2024-07-09', -200.22, 'PAGAMENTO', 'Cinema')
]

new_df = spark.createDataFrame(new_data, schema)

# Convertendo a coluna 'data_transacao' para DateType
new_df = new_df.withColumn("data_transacao", to_date(col("data_transacao"), "yyyy-MM-dd"))


# Adicionando os novos dados à tabela Delta
new_df.write \
    .format("delta") \
    .mode("append") \
    .save(delta_path)

# Verificando se os dados foram adicionados corretamente
spark.sql(f"SELECT * FROM delta.`{delta_path}`").show()


# In[15]:


# Realizar uma consulta para selecionar todas as transações com valor acima de 1000
df_high_value_updated = spark.sql("SELECT * FROM financial_data WHERE valor > 1000")
df_high_value_updated.show()


# In[17]:


# Carregando a tabela Delta
df = spark.read.format("delta").load(delta_path)

# Registrando o DataFrame como uma tabela temporária para consulta SQL
df.createOrReplaceTempView("financial_data")

# Executando a consulta SQL para agrupar as transações por tipo e somar os valores
result_df = spark.sql("""
    SELECT tipo_transacao, SUM(valor) as total_valor
    FROM financial_data
    GROUP BY tipo_transacao
""")

# Mostrando o resultado
result_df.show()

# Parando a sessão Spark
spark.stop()


# In[ ]:




