# Use official slim Python base image
FROM python:3.12-slim

# Set working directory in container
WORKDIR /app

# Copy source
COPY . /app

# Install dependencies (empty but kept for later libs)
RUN pip install -r requirements.txt

# Ensure data directory exists
RUN mkdir -p /app/data

# Default command (execute the event generator once)
CMD ["python", "-m", "app.main"]
