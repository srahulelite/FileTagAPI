FROM python:3.11-slim

# system deps (ffmpeg included)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates build-essential libpq-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy requirements (adjust if you use pyproject/poetry)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# create uploads dir
RUN mkdir -p /app/uploads
VOLUME ["/app/uploads"]

ENV PORT=8080
EXPOSE ${PORT}

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
