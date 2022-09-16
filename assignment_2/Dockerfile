FROM python:3.9

RUN pip install --upgrade google-cloud-storage python-dotenv google-cloud-pubsub

COPY . /app

WORKDIR /app

CMD ["python", "-u", "main.py"]