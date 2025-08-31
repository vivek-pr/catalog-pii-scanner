# syntax=docker/dockerfile:1.7-labs
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System deps
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --uid 1000 appuser

# Copy only metadata first for better caching
COPY pyproject.toml README.md ./
COPY src ./src

# Install package
RUN pip install .

# Switch to non-root
USER appuser

# Default entrypoint is the CLI
ENTRYPOINT ["cps"]
CMD ["--help"]

