FROM python:3.10-slim AS base

RUN mkdir -p /app
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

FROM base AS deploy
WORKDIR /app
COPY . /app/
CMD ["python", "main.py"]