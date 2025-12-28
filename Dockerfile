FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create data directory
RUN mkdir -p data

# Expose port
EXPOSE 8000

# Script to generate data if missing and start app
RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]
