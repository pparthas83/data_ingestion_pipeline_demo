# Base launcher image provided by GCP for Python Dataflow Flex Templates
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# Define environment variables
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"

# Create template directory
WORKDIR /template

# Copy requirements and install dependencies
COPY requirements.txt /template/requirements.txt
COPY main.py /template/main.py

# Install dependencies into launcher environment
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /template/requirements.txt

# Ensure template entrypoint is set
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
