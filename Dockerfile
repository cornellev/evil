FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schema/ schema/
COPY src/ src/

ENV PYTHONPATH=/app/src
ENV EVIL_DB_PATH=/data/evil.db
ENV EVIL_MCP_HOST=0.0.0.0
ENV EVIL_MCP_PORT=8765

EXPOSE 8765
VOLUME ["/data"]

CMD ["python", "-m", "evil.mcp_server"]
