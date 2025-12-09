# Usa una imagen base oficial de Python
FROM python:3.10-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos de requisitos e instálalos
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación
COPY . .

# Expone el puerto que usa Flask
EXPOSE 5000

# Comando para ejecutar la aplicación cuando el contenedor se inicie
CMD ["flask", "run", "--host=0.0.0.0"]
