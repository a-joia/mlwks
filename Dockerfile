FROM python:3.11-slim

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./common /app/common
COPY ./entrypoint.py /app/entrypoint.py

ENTRYPOINT ["python", "entrypoint.py"]