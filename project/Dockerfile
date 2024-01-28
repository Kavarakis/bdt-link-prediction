# This is a muti-stage Dockerfile that can be used to build many different types of
# bundled dependencies for PySpark projects. 
# The `base` stage installs generic tools necessary for packaging.
#
# There are `export-` and `build-` stages for the different types of projects.
# - python-packages - Generic support for Python projects with pyproject.toml
# - poetry - Support for Poetry projects
#
# This Dockerfile is generated automatically as part of the emr-cli tool.
# Feel free to modify it for your needs, but leave the `build-` and `export-`
# stages related to your project.
#
# To build manually, you can use the following command, assuming 
# the Docker BuildKit backend is enabled. https://docs.docker.com/build/buildkit/
#
# Example for building a poetry project and saving the output to dist/ folder
# docker build --target export-poetry --output dist .


# Use the Amazon EMR Serverless Spark base image
FROM --platform=linux/amd64 public.ecr.aws/emr-serverless/spark/emr-7.0.0:latest AS base

# Run as root user for installation purposes
USER root

# Print Python version for confirmation
RUN python3 --version

# Create a virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Set Python path for PySpark
ENV PYSPARK_PYTHON=/usr/bin/python3

# Upgrade pip in the virtual environment
RUN python3 -m pip install --upgrade pip

# Copy your application code to the container
WORKDIR /app
COPY . .

# Install dependencies including venv-pack
# No need for sudo as you're already root
RUN python3 -m pip install venv-pack==0.2.0
RUN python3 -m pip install .

# Package the virtual environment with venv-pack
RUN mkdir /output && venv-pack -f -o /output/pyspark_deps.tar.gz

# Switch to non-root user for running the application
USER hadoop:hadoop

# The packaged virtual environment is now ready to be used


# Export stage - used to copy packaged venv to local filesystem
FROM scratch AS export-python
COPY --from=base /output/pyspark_deps.tar.gz /
