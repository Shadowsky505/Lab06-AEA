from fastapi import FastAPI, HTTPException
import polars as pl
from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.master("spark://54.242.121.50:7077").appName("FastAPI-Spark").getOrCreate()

app = FastAPI()

# Cargar el archivo CSV con Polars y Spark
try:
    # Cargar el archivo CSV con Polars primero para tenerlo listo y después usarlo en Spark
    data = pl.read_csv("data.csv", separator=";", truncate_ragged_lines=True)
    # Convertir los datos de Polars a un DataFrame de Spark
    spark_df = spark.createDataFrame(data.to_pandas())
except FileNotFoundError:
    raise Exception("El archivo data.csv no fue encontrado.")

@app.get("/")
def root():
    return {"message": "API activa con datos usando Polars y Spark"}

@app.get("/data/")
def get_data(limit: int = 10):
    """
    Retorna los primeros `limit` registros del archivo CSV procesados con Spark.
    """
    if limit > len(data):
        raise HTTPException(status_code=400, detail="El límite excede el tamaño del archivo")
    
    # Filtrar los primeros 'limit' registros utilizando Spark
    result = spark_df.limit(limit).toPandas().to_dict(orient="records")

    return result

@app.get("/data/{index}")
def get_data_by_index(index: int):
    """
    Retorna un registro específico por índice, usando Spark para procesamiento distribuido.
    """
    if index < 0 or index >= len(data):
        raise HTTPException(status_code=404, detail="Índice fuera de rango")
    
    # Obtener el registro por índice utilizando Spark
    result = spark_df.collect()[index].asDict()

    return result
