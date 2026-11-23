FROM python:3.12-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN pip install --no-cache-dir openpyxl==3.1.5 et-xmlfile==2.0.0
COPY export.py /app/export.py
ENTRYPOINT ["python", "/app/export.py"]
