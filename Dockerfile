FROM golang:1.25-bookworm AS builder

WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus-dev \
    libopusfile-dev \
    libsoxr-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o /out/talk-go .

FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libopus0 \
    libopusfile0 \
    libsoxr0 \
    libstdc++6 \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=builder /out/talk-go /app/talk-go
COPY daily_bridge.py /app/daily_bridge.py
COPY jinja_renderer.py /app/jinja_renderer.py
COPY clients /app/clients
COPY background-office-sound.mp3 /app/background-office-sound.mp3

EXPOSE 7860

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/app/talk-go"]
