FROM python:3.12-slim

# Install rclone and fuse
RUN apt-get update && apt-get install -y \
    fuse \
    curl \
    unzip \
    && curl -O https://downloads.rclone.org/rclone-current-linux-amd64.zip \
    && unzip rclone-current-linux-amd64.zip \
    && cp rclone-*/rclone /usr/bin/ \
    && chown root:root /usr/bin/rclone \
    && chmod 755 /usr/bin/rclone \
    && rm -rf rclone-* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

ADD src/ /app/src
COPY entrypoint.sh /app/

# Create mount point
RUN mkdir -p /data

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
