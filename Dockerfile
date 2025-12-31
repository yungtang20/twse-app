# Use a multi-stage build to keep the final image small
# Stage 1: Build Frontend
FROM node:18-alpine as frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Stage 2: Setup Backend and Run
FROM python:3.10-slim
WORKDIR /app

# Install system dependencies (if any needed for sqlite/pandas)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy backend requirements
COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# Copy backend code
COPY backend/ ./backend/

# Copy necessary root scripts
COPY 最終修正.py .
COPY cloud_update.py .
COPY update_institutional_streaks.py .

# Copy built frontend assets from Stage 1
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist

# Copy other necessary files (like config if needed, though config.json is usually gitignored)
# COPY config.json . 

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

# Expose the port
EXPOSE 8000

# Run the application
CMD ["python", "backend/main.py"]
