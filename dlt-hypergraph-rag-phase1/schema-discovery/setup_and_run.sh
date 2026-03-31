
#!/bin/bash

echo "🚀 Setting up Schema Discovery Project..."

# Install Python dependencies
echo "📦 Installing Python packages..."
pip install -r requirements.txt

# Start Docker services
echo "🐳 Starting PostgreSQL and SeaweedFS..."
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 10

# Run schema discovery
echo "🔍 Running Schema Discovery..."
python run_discovery.py

echo "✅ Setup complete! Check the output/ directory for reports."
