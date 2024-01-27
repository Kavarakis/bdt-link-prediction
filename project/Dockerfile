# Use Python 3.10.6 slim image as base
FROM python:3.10.6-slim as base

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Set up the virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV --copies
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies and packages
RUN pip install --upgrade pip
RUN pip install venv-pack==0.2.0
RUN pip install poetry
ENV PATH="$PATH:/root/.local/bin"

# Install application dependencies using Poetry
# If you have a pyproject.toml and poetry.lock file, uncomment the following lines
# COPY pyproject.toml poetry.lock ./
# RUN poetry install --no-dev

# Build and package the environment
RUN mkdir /output && venv-pack -o /output/pyspark_deps.tar.gz --python-prefix $VIRTUAL_ENV

# Define the export-python stage
FROM scratch AS export-python
COPY --from=base /output/pyspark_deps.tar.gz /
