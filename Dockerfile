FROM python:3.12

RUN pip install poetry

WORKDIR /app

COPY pyproject.toml /app/
RUN poetry config virtualenvs.create false
RUN poetry install --no-root --no-interaction --no-ansi

COPY . /app

CMD ["python", "romania_parser/main.py"]