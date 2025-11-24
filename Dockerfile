# Multi-stage Dockerfile for ELT Project
FROM python:3.13-slim as base

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY pyproject.toml ./

# Install uv package manager
RUN pip install uv

# Install Python dependencies
RUN uv pip install --system -r pyproject.toml

# Copy application code
COPY data_ingestion/ ./data_ingestion/
COPY data_transformation/ ./data_transformation/

# Accept build arguments for secrets
ARG DATABRICKS_HOST
ARG DATABRICKS_HTTP_PATH
ARG DATABRICKS_TOKEN
ARG AIRBYTE_CLIENT_ID
ARG AIRBYTE_CLIENT_SECRET
ARG AIRBYTE_WORKSPACE_ID

# Set environment variables (these will be available at runtime)
ENV DATABRICKS_HOST=${DATABRICKS_HOST}
ENV DATABRICKS_HTTP_PATH=${DATABRICKS_HTTP_PATH}
ENV DATABRICKS_TOKEN=${DATABRICKS_TOKEN}
ENV AIRBYTE_CLIENT_ID=${AIRBYTE_CLIENT_ID}
ENV AIRBYTE_CLIENT_SECRET=${AIRBYTE_CLIENT_SECRET}
ENV AIRBYTE_WORKSPACE_ID=${AIRBYTE_WORKSPACE_ID}

# Create profiles.yml from environment variables
RUN mkdir -p /root/.dbt && \
    cat > /root/.dbt/profiles.yml <<EOF
dvd_rental:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: workspace
      schema: dvd_rental
      host: ${DATABRICKS_HOST}
      http_path: ${DATABRICKS_HTTP_PATH}
      token: ${DATABRICKS_TOKEN}
      threads: 4
EOF

# Health check (optional)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command (can be overridden)
CMD ["/bin/bash"]

