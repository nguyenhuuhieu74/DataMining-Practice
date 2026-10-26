FROM python:3.14-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUTF8=1
WORKDIR /app
RUN pip install --no-cache-dir numpy==2.5.3 pandas==3.0.5 scipy==1.18.1 scikit-learn==1.9.1
COPY recommendation/src /app/recommendation/src
COPY recommendation/web /app/recommendation/web
COPY recommendation/config.json recommendation/protocol.json /app/recommendation/
EXPOSE 8501
CMD ["python", "recommendation/src/web.py", "--host", "0.0.0.0", "--port", "8501"]
