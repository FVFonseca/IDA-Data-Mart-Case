FROM python:3.11.12-bookworm

WORKDIR /app

COPY requirements.txt .

RUN pip install --cache-dir /pip-cache -r requirements.txt

COPY . .

CMD ["python", "main.py"]