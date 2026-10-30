#!/bin/bash
# Activation script for NornicDB neural training environment
# Usage: source activate.sh

# Activate virtual environment
source venv/bin/activate

# Set environment variables for Apple Silicon
export PYTORCH_ENABLE_MPS_FALLBACK=1

# Set HuggingFace cache location (optional)
export HF_HOME="${HOME}/.cache/huggingface"

# Display status
echo "🧠 NornicDB Neural Training Environment"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python -c "
import torch
print('Python:', torch.__version__.split('+')[0])
print('PyTorch:', torch.__version__)
print('Hardware:', 'MPS (Apple Silicon)' if torch.backends.mps.is_available() else 'CPU Only')
print('Ready: ✓')
"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Quick commands:"
echo "  Generate dataset: python scripts/generate_heimdall_dataset.py --repo .. --output data/heimdall.jsonl"
echo "  Train model:      python train.py --preset heimdall --dataset data/heimdall.jsonl"
echo "  Export GGUF:      python export_to_gguf.py --model_dir models/heimdall --output heimdall-q4.gguf"
echo ""
echo "Documentation: README.md, HEIMDALL.md, SETUP.md"
