FROM python:3.9

# Set the working directory in the container
WORKDIR /PostgresDocker

# Copy the current directory contents into the container at /app
COPY . /PostgresDocker

# Install any needed packages specified in requirements.txt
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir -r requirements.txt


# Run script.py when the container launches
CMD ["python", "ClassicModels_ETL.py"]
