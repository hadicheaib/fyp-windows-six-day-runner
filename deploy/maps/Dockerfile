FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/opt/playwright \
    HOME=/tmp/collector

WORKDIR /app
COPY pyproject.toml ./
COPY deploy/maps/requirements.txt ./deploy/maps/requirements.txt
# Keep third-party packages and the headless browser cached across source edits.
RUN python -c "import pathlib,tomllib; p=pathlib.Path; p('/tmp/project-requirements.txt').write_text(chr(10).join(tomllib.loads(p('pyproject.toml').read_text())['project']['dependencies']))" \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata \
    && pip install --no-cache-dir -r /tmp/project-requirements.txt -r deploy/maps/requirements.txt \
    && python -m playwright install --with-deps --only-shell chromium \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin collector \
    && chmod -R a+rX /opt/playwright

COPY src ./src
RUN pip install --no-cache-dir --no-deps .

USER 10001:10001
ENTRYPOINT ["python", "-m", "lebanon_discovery.cloud_maps"]
