#!/bin/bash
# Manually download Qwen 0.5B Instruct model using huggingface-cli

set -e

echo "Downloading Qwen 0.5B Instruct model manually..."
echo ""

# Check if huggingface-cli is installed
if ! command -v huggingface-cli &> /dev/null; then
    echo "Installing huggingface_hub..."
    pip install --upgrade huggingface_hub
fi

# Set model cache directory
CACHE_DIR="${HOME}/.cache/huggingface/hub"
MODEL_NAME="Qwen/qwen3-0.6b-Instruct"

echo "Model: $MODEL_NAME"
echo "Cache directory: $CACHE_DIR"
echo ""

# Download model
echo "Downloading model (this may take a while)..."
huggingface-cli download $MODEL_NAME --local-dir "$CACHE_DIR/models--$(echo $MODEL_NAME | tr '/' '--')" --local-dir-use-symlinks False

echo ""
echo "✓ Model downloaded successfully!"
echo ""
echo "You can now use it with:"
echo "  python3 train_nornicdb_cypher.py --train-only --local-model $CACHE_DIR/models--$(echo $MODEL_NAME | tr '/' '--')"
