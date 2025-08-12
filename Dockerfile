FROM python:3.12.9-slim

WORKDIR /app
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN playwright install --with-deps

COPY . .

CMD [ "python", "main.py" ]