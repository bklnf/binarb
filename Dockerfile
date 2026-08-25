FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY binarb ./binarb
COPY tests ./tests
RUN pip install --no-cache-dir ".[dev]" && pytest -q

CMD ["python", "-m", "binarb", "run"]
