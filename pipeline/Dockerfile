# Base launcher image provided by GCP for Python Dataflow Flex Templates
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# Define environment variables
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"

# Create template directory
WORKDIR /template

# Copy requirements, setup file, and package files
COPY requirements.txt /template/requirements.txt
COPY setup.py /template/setup.py
COPY gcs_etl /template/gcs_etl
COPY main.py /template/main.py

# Install OpenJDK runtime, dependencies, and gcs_etl package into global site-packages
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /template/requirements.txt \
    && pip install --no-cache-dir /template

# Ensure template entrypoint is set
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
