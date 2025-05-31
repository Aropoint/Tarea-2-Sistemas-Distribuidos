FROM python:3.9-slim

WORKDIR /app
COPY filtering.py .

# Instala dependencias
RUN pip install pymongo pandas geopy

CMD ["python", "filtering.py"]
