FROM mcr.microsoft.com/devcontainers/base:bullseye

# Install distutils so 'uv' can inspect & package Python
USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-distutils \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /uvx /bin/

USER vscode

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV UV_PYTHON_VERSION=3.11

WORKDIR /app

# Install uv python3.11
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./uv.lock,target=/uv.lock \
    --mount=type=bind,source=./pyproject.toml,target=/pyproject.toml \
    uv python install ${UV_PYTHON_VERSION}

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=./uv.lock,target=/uv.lock \
    --mount=type=bind,source=./pyproject.toml,target=/pyproject.toml \
    uv sync --frozen --no-install-project
