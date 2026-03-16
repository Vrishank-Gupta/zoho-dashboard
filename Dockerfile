FROM python:3.14.2-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV QUBO_APP_HOST=0.0.0.0
ENV QUBO_APP_PORT=8000
ENV QUBO_APP_RELOAD=false
ENV QUBO_SERVE_FRONTEND=false

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python", "-m", "qubo_dashboard.run"]
