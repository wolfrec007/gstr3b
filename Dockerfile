# Use the official Streamlit Docker image as base
FROM python:3.12-slim

# Install system dependencies including Java for tabula-py
RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create necessary directories
RUN mkdir -p uploads exports

# Render injects PORT env var; default to 7860 for local
ENV PORT=7860

# Expose the port
EXPOSE $PORT

# Set Streamlit config - use $PORT from Render
ENV STREAMLIT_SERVER_PORT=$PORT
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_ENABLE_CORS=false
ENV STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Run the app
CMD streamlit run app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false