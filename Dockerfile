# Use the official Python image from Docker Hub
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install additional packages that might be useful
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl wget vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create directory for credentials
RUN mkdir -p /app/credentials

# Copy the application code into the container
COPY . .

# Create an entrypoint script to run both programs in sequence
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "Starting data scraping..."\n\
python main.py\n\
\n\
echo "Starting BigQuery data upload..."\n\
python dump_bigq.py\n\
\n\
echo "Process completed successfully!"\n\
' > /app/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Set the entrypoint script as the default command
ENTRYPOINT ["/app/entrypoint.sh"]