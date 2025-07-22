FROM apache/airflow:3.0.3-python3.10

RUN pip install --no-cache-dir --upgrade pip 

WORKDIR /opt/airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt






