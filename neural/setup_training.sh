#!/bin/bash
# Setup script for NornicDB Cypher training environment

set -e

echo "Setting up NornicDB Cypher training environment..."
echo ""

# Check if we're in the neural directory
if [ ! -f "train_nornicdb_cypher.py" ]; then
    echo "ERROR: Please run this script from the neural/ directory"
    exit 1
fi

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 not found. Please install Python 3.8+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $PYTHON_VERSION"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip --quiet

# Install dependencies
echo ""
echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

echo ""
echo "✓ Setup complete!"
echo ""
echo "To activate the environment in the future, run:"
echo "  source venv/bin/activate"
echo ""
echo "Then you can run:"
echo "  python3 train_nornicdb_cypher.py --generate-data --train --export"
echo ""
