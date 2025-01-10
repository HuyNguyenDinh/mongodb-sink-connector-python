FROM python:3.10-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN mkdir -p /app
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN uv pip install --system -r /app/requirements.txt

FROM base AS deploy
WORKDIR /app
COPY . /app/
EXPOSE 8000
CMD ["python", "main.py"]