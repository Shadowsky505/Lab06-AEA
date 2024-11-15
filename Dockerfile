# Usa una imagen base de Python
FROM python:3.9.9-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos necesarios para el proyecto
COPY . /app

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto 8000 para la API
EXPOSE 8000

# Define el comando para iniciar la API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
