FROM python:3.11-alpine

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "producer.py"]