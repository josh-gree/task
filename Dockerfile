FROM python:3.8

RUN mkdir /app 
COPY . /app

WORKDIR /app

RUN pip3 install poetry
RUN poetry install --no-dev

CMD poetry run python src/main.py