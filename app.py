from fastapi import FastAPI, HTTPException
import polars as pl
from dask.distributed import Client, progress

# Conectar con el clúster de Dask
client = Client('scheduler-address:8786')  # Cambia esto según la dirección del nodo maestro

app = FastAPI()

# Carga el archivo CSV con Polars
try:
    data = pl.read_csv("data.csv", separator=";", truncate_ragged_lines=True)
except FileNotFoundError:
    raise Exception("El archivo data.csv no fue encontrado.")

@app.get("/")
def root():
    return {"message": "API activa con datos usando Polars y Dask"}

@app.get("/data/")
def get_data(limit: int = 10):
    """
    Retorna los primeros `limit` registros del archivo CSV distribuidos con Dask.
    """
    if limit > len(data):
        raise HTTPException(status_code=400, detail="El límite excede el tamaño del archivo")
    
    # Usando Dask para procesamiento distribuido
    future = client.submit(lambda: data[:limit].to_dicts())
    result = future.result()  # Espera la ejecución del cálculo distribuido

    return result

@app.get("/data/{index}")
def get_data_by_index(index: int):
    """
    Retorna un registro específico por índice, usando Dask para procesamiento distribuido.
    """
    if index < 0 or index >= len(data):
        raise HTTPException(status_code=404, detail="Índice fuera de rango")
    
    # Usando Dask para procesamiento distribuido
    future = client.submit(lambda: data[index].to_dict())
    result = future.result()  # Espera la ejecución del cálculo distribuido

    return result
