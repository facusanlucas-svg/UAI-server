FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server_ws.py .

# Ejecuta el servidor WebSocket (Render inyecta la variable de entorno PORT)
CMD ["python", "server_ws.py"]
