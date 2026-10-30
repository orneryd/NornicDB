# Installation & Setup Guide

Complete setup instructions for training neural models with NornicDB.

---

## What Are We Training?

**TL;DR:** We **fine-tune existing models** (like TinyLlama, Qwen, Phi-3), we do **NOT train from scratch**.

### Fine-Tuning vs Training From Scratch

| Aspect | Fine-Tuning (LoRA) ✅ **What we do** | Training From Scratch ❌ |
|--------|--------------------------------------|--------------------------|
| **Base model** | Use pre-trained model (Qwen2.5-1.5B) | Create new model architecture |
| **Trainable params** | 1-2% (~20M of 1.5B) | 100% (~1.5B parameters) |
| **Training time** | 6-8 hours on 2080 Ti | Weeks on GPU cluster |
| **Data needed** | 1K-10K examples | Millions of examples |
| **VRAM required** | 8-11GB (consumer GPU) | 80GB+ (datacenter GPU) |
| **Cost** | $0 (use your GPU) | $10,000+ (cloud GPUs) |
| **Results** | Excellent for specialization | Requires massive datasets |

### Why LoRA Fine-Tuning is Better

**LoRA (Low-Rank Adaptation)** adds small "adapter" layers to pre-trained models:

1. ✅ **Base model stays frozen** - Already understands language
2. ✅ **Only train adapters** - Small, fast, efficient  
3. ✅ **Specializes knowledge** - Learns Cypher/NornicDB without forgetting English
4. ✅ **Works on consumer hardware** - Your 2080 Ti or M1 Mac is enough
5. ✅ **Fast iteration** - Try different datasets in hours, not weeks

**Real example:** Heimdall fine-tunes Qwen2.5-1.5B (already trained on trillions of tokens) to become a Cypher expert in 8 hours with just 5,000 examples.

---

## Prerequisites

Before starting, ensure you have:

- **Python 3.8 or later** (Python 3.10+ recommended)
- **Git** (for cloning repository)
- **10GB+ free disk space** (for models and datasets)
- **NVIDIA GPU** (optional, but highly recommended)
  - CUDA 11.8+ for GPU acceleration
  - cuDNN for optimized operations
- **macOS with Apple Silicon** (optional, for Metal acceleration)
  - M1/M2/M3 chip with Metal support

---

## Installation Steps

### 1. Verify Python Installation

Check your Python version:

```bash
python3 --version
```

**Expected output:** `Python 3.10.x` or `Python 3.11.x` (3.8+ works)

If you don't have Python 3.8+:

**macOS (Homebrew):**
```bash
brew install python@3.11
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev
```

**Windows:**
Download from [python.org](https://www.python.org/downloads/) and install.

---

### 2. Navigate to Neural Directory

```bash
cd /path/to/NornicDB/neural
```

For this repository:
```bash
cd /Users/timothysweet/src/NornicDB/neural
```

---

### 3. Create Virtual Environment

**Why?** Isolates dependencies from system Python, prevents conflicts.

```bash
python3 -m venv venv
```

This creates a `venv/` directory with an isolated Python environment.

---

### 4. Activate Virtual Environment

**macOS/Linux:**
```bash
source venv/bin/activate
```

**Windows (PowerShell):**
```powershell
.\venv\Scripts\Activate.ps1
```

**Windows (Command Prompt):**
```cmd
.\venv\Scripts\activate.bat
```

Your prompt should now show `(venv)` prefix:
```bash
(venv) ➜ neural git:(main) ✗
```

---

### 5. Upgrade pip

Ensure you have the latest pip:

```bash
pip install --upgrade pip
```

**Expected output:**
```
Successfully installed pip-25.3
```

---

### 6. Install PyTorch (GPU or CPU)

PyTorch installation varies by platform. Choose your configuration:

#### **Option A: NVIDIA GPU (CUDA)**

For CUDA 11.8:
```bash
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

For CUDA 12.1:
```bash
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121
```

Verify CUDA:
```bash
python -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}')"
```

Should print: `CUDA available: True`

#### **Option B: Apple Silicon (Metal/MPS)**

```bash
pip install torch torchvision torchaudio
```

Verify Metal:
```bash
python -c "import torch; print(f'MPS available: {torch.backends.mps.is_available()}')"
```

Should print: `MPS available: True`

#### **Option C: CPU Only**

```bash
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
```

**Note:** CPU training is very slow. Only use for testing.

---

### 7. Install Training Dependencies

Install all required packages:

```bash
pip install -r requirements.txt
```

This installs:
- `transformers` - HuggingFace models
- `peft` - LoRA/QLoRA training
- `datasets` - Dataset loading
- `accelerate` - Multi-GPU support
- `bitsandbytes` - Quantization (CUDA only)
- `safetensors` - Model serialization
- `tensorboard` - Training visualization
- And more...

**Expected duration:** 2-5 minutes depending on internet speed.

---

### 8. Install Flash Attention 2 (Optional, NVIDIA only)

For faster training on NVIDIA GPUs:

```bash
pip install flash-attn --no-build-isolation
```

**Note:** This requires CUDA toolkit installed. If it fails, training will still work (just slower).

---

### 9. Install llama.cpp (For GGUF Export)

Clone llama.cpp for model export:

```bash
cd /Users/timothysweet/src/NornicDB/neural
git clone https://github.com/ggerganov/llama.cpp
cd llama.cpp
make
```

**For macOS with Metal acceleration:**
```bash
make LLAMA_METAL=1
```

**For CUDA:**
```bash
make LLAMA_CUBLAS=1
```

This enables converting trained models to GGUF format.

---

### 10. Verify Installation

Test that everything works:

```bash
cd /Users/timothysweet/src/NornicDB/neural

# Check PyTorch
python -c "import torch; print(f'PyTorch: {torch.__version__}')"

# Check Transformers
python -c "import transformers; print(f'Transformers: {transformers.__version__}')"

# Check hardware
python -c "
import torch
print(f'CUDA available: {torch.cuda.is_available()}')
print(f'MPS available: {torch.backends.mps.is_available() if hasattr(torch.backends, \"mps\") else False}')
print(f'CPU cores: {torch.get_num_threads()}')
"
```

**Expected output:**
```
PyTorch: 2.1.0+cu118  (or similar)
Transformers: 4.36.0  (or similar)
CUDA available: True  (or False if CPU/MPS)
MPS available: False  (or True on Apple Silicon)
CPU cores: 16  (varies by system)
```

---

### 11. Create Data Directory

```bash
mkdir -p data
```

This is where training datasets will be stored.

---

## Quick Test

Test the installation with a simple command:

```bash
# Check available presets
python train.py --help

# Estimate memory for Heimdall
python train.py --preset heimdall --estimate_memory
```

**Expected output:**
```
Model: Qwen/Qwen2.5-1.5B
LoRA Parameters: ~25M
Estimated VRAM: 8.2 GB
Recommended: batch_size=8, gradient_accumulation=2
```

---

## Environment Variables (Optional)

### For Apple Silicon (Metal)

Enable fallback for unsupported operations:
```bash
export PYTORCH_ENABLE_MPS_FALLBACK=1
```

Add to `~/.zshrc` or `~/.bashrc` to make permanent:
```bash
echo 'export PYTORCH_ENABLE_MPS_FALLBACK=1' >> ~/.zshrc
```

### For NVIDIA GPU

Set CUDA device (if you have multiple GPUs):
```bash
export CUDA_VISIBLE_DEVICES=0  # Use first GPU
```

### For HuggingFace

Cache models locally to avoid re-downloading:
```bash
export HF_HOME=/Users/timothysweet/.cache/huggingface
```

---

## Complete Setup Script

Save this as `setup.sh` for easy setup:

```bash
#!/bin/bash
set -e

echo "🚀 Setting up NornicDB Neural Training Environment"

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "✓ Python $PYTHON_VERSION detected"

# Navigate to neural directory
cd "$(dirname "$0")"

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Detect platform and install PyTorch
echo "🔍 Detecting hardware..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    if sysctl -n machdep.cpu.brand_string | grep -q "Apple"; then
        echo "🍎 Apple Silicon detected - installing PyTorch with MPS support"
        pip install torch torchvision torchaudio
        export PYTORCH_ENABLE_MPS_FALLBACK=1
    else
        echo "🍎 Intel Mac detected - installing CPU-only PyTorch"
        pip install torch torchvision torchaudio
    fi
elif command -v nvidia-smi &> /dev/null; then
    echo "🎮 NVIDIA GPU detected - installing PyTorch with CUDA"
    pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
else
    echo "💻 No GPU detected - installing CPU-only PyTorch"
    pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
fi

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Try to install flash-attention (may fail on non-CUDA systems)
if command -v nvidia-smi &> /dev/null; then
    echo "⚡ Installing Flash Attention 2..."
    pip install flash-attn --no-build-isolation || echo "⚠️  Flash Attention install failed (not critical)"
fi

# Create data directory
echo "📁 Creating data directory..."
mkdir -p data

# Clone llama.cpp if not exists
if [ ! -d "llama.cpp" ]; then
    echo "🦙 Cloning llama.cpp for GGUF export..."
    git clone https://github.com/ggerganov/llama.cpp
    cd llama.cpp
    
    if [[ "$OSTYPE" == "darwin"* ]] && sysctl -n machdep.cpu.brand_string | grep -q "Apple"; then
        make LLAMA_METAL=1
    elif command -v nvidia-smi &> /dev/null; then
        make LLAMA_CUBLAS=1
    else
        make
    fi
    cd ..
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Activate environment: source venv/bin/activate"
echo "  2. Generate dataset: python scripts/generate_heimdall_dataset.py --repo .. --output data/heimdall.jsonl"
echo "  3. Train model: python train.py --preset heimdall --dataset data/heimdall.jsonl"
echo ""
echo "Documentation: README.md and docs/"
```

Make it executable:
```bash
chmod +x setup.sh
./setup.sh
```

---

## Troubleshooting

### Problem: `pip` command not found after activation

**Solution:**
```bash
deactivate  # Exit venv
rm -rf venv  # Remove corrupted venv
python3 -m venv venv  # Recreate
source venv/bin/activate
```

### Problem: PyTorch CUDA not available

**Solution:**
Check CUDA installation:
```bash
nvidia-smi  # Should show GPU info
nvcc --version  # Should show CUDA version
```

Reinstall PyTorch with correct CUDA version:
```bash
pip uninstall torch torchvision torchaudio
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

### Problem: `ImportError: No module named 'transformers'`

**Solution:**
Ensure virtual environment is activated:
```bash
which python  # Should point to venv/bin/python
pip list | grep transformers  # Should show transformers
```

If not installed:
```bash
pip install transformers
```

### Problem: Out of memory during training

**Solution:**
1. Use QLoRA: `--use_qlora`
2. Reduce batch size: `--batch_size 2`
3. Increase gradient accumulation: `--gradient_accumulation 16`
4. Use smaller model: `--preset qwen_0_5b`

### Problem: Flash Attention installation fails

**Solution:**
This is optional. Training works without it (just slower). Skip if installation fails.

### Problem: llama.cpp compilation fails

**Solution:**
Install build tools:

**macOS:**
```bash
xcode-select --install
```

**Ubuntu:**
```bash
sudo apt install build-essential
```

---

## Updating Dependencies

To update all packages:

```bash
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade -r requirements.txt
```

To update just PyTorch:
```bash
pip install --upgrade torch torchvision torchaudio
```

---

## Uninstalling

To completely remove the environment:

```bash
cd /Users/timothysweet/src/NornicDB/neural
deactivate  # Exit venv if active
rm -rf venv  # Remove virtual environment
rm -rf llama.cpp  # Remove llama.cpp
rm -rf data  # Remove datasets (optional)
rm -rf models  # Remove trained models (optional)
```

---

## Next Steps

After successful installation:

1. **Generate training data:**
   ```bash
   python scripts/generate_heimdall_dataset.py --repo .. --output data/heimdall.jsonl
   ```

2. **Validate dataset:**
   ```bash
   python scripts/validate_dataset.py data/heimdall.jsonl
   ```

3. **Train model:**
   ```bash
   python train.py --preset heimdall --dataset data/heimdall.jsonl --output_dir models/heimdall
   ```

4. **Export to GGUF:**
   ```bash
   python export_to_gguf.py --model_dir models/heimdall --output heimdall-q4.gguf
   ```

See [HEIMDALL.md](HEIMDALL.md) for complete training guide.

---

## System Requirements

### Minimum Requirements
- Python 3.8+
- 8 GB RAM
- 20 GB disk space
- CPU-only (very slow)

### Recommended for Training
- Python 3.10+
- 16 GB RAM
- NVIDIA GPU with 8+ GB VRAM (2080 Ti, 3060, etc.)
- 50 GB disk space
- CUDA 11.8+

### Optimal for Training
- Python 3.11
- 32 GB RAM
- NVIDIA GPU with 16+ GB VRAM (4090, A100)
- 100 GB disk space (for multiple models)
- CUDA 12.1+
- NVMe SSD for fast I/O

### Apple Silicon
- macOS 12.3+ (for MPS support)
- M1/M2/M3 chip
- 16+ GB unified memory
- 50 GB disk space

---

## Common Workflows

### Complete Training Workflow

```bash
# 1. Activate environment (do this every time)
cd /Users/timothysweet/src/NornicDB/neural
source activate.sh  # Or: source venv/bin/activate

# 2. Generate training dataset
python scripts/generate_heimdall_dataset.py \
    --repo .. \
    --output data/heimdall_training.jsonl \
    --num-examples 5000

# 3. Validate raw dataset
python scripts/validate_dataset.py data/heimdall_training.jsonl

# 4. Auto-correct (REQUIRED - edits in-place with backup)
python scripts/correct_dataset.py data/heimdall_training.jsonl \
    --min-category-size 50 \
    --max-category-ratio 3.0

# 5. Validate corrected dataset (should show <3:1 ratio)
python scripts/validate_dataset.py data/heimdall_training.jsonl

# 6. Choose variant and train (LoRA fine-tuning, NOT from scratch)

# Option A: Apache 2.0 variant (faster, smaller, recommended)
python train.py \
    --preset heimdall \
    --dataset data/heimdall_training.jsonl \
    --output_dir models/heimdall \
    --epochs 10

# Option B: MIT variant (higher quality, larger, MIT license)
python train.py \
    --preset heimdall_mit \
    --dataset data/heimdall_training.jsonl \
    --output_dir models/heimdall_mit \
    --epochs 10

# 7. Export to GGUF format
# For Apache 2.0:
python export_to_gguf.py \
    --model_dir models/heimdall \
    --output heimdall-q4.gguf \
    --quant_type Q4_K_M

# For MIT:
python export_to_gguf.py \
    --model_dir models/heimdall_mit \
    --output heimdall-mit-q4.gguf \
    --quant_type Q4_K_M

# 8. Deploy to NornicDB
mkdir -p ../plugins/heimdall/models
# Copy the variant you trained:
cp heimdall-q4.gguf ../plugins/heimdall/models/
# OR: cp heimdall-mit-q4.gguf ../plugins/heimdall/models/

# Deactivate when done
deactivate
```

### Quick Dataset Testing

```bash
source activate.sh

# Generate small test dataset
python scripts/generate_heimdall_dataset.py \
    --num-examples 100 \
    --output data/test.jsonl

# Validate
python scripts/validate_dataset.py data/test.jsonl

# Quick training test (1 epoch)
python train.py --preset heimdall --dataset data/test.jsonl --epochs 1
```

### Dataset Correction Only

```bash
source activate.sh

# Auto-correct existing dataset
python scripts/correct_dataset.py data/old_dataset.jsonl \
    --output data/corrected_dataset.jsonl \
    --backup

# Check improvements
python scripts/validate_dataset.py data/corrected_dataset.jsonl
```

### Adding New Dependencies

```bash
source venv/bin/activate
pip install new-package
pip freeze > requirements.txt  # Update requirements file
```

### Switching Python Versions

```bash
deactivate
rm -rf venv
python3.11 -m venv venv  # Use specific Python version
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

## IDE Setup

### VS Code

1. Install Python extension
2. Open neural directory: `code /Users/timothysweet/src/NornicDB/neural`
3. Select Python interpreter: `Cmd+Shift+P` → "Python: Select Interpreter" → Choose `./venv/bin/python`
4. Install recommended extensions:
   - Python (ms-python.python)
   - Pylance (ms-python.vscode-pylance)
   - Jupyter (ms-toolsai.jupyter)

### PyCharm

1. Open project: `/Users/timothysweet/src/NornicDB/neural`
2. Configure interpreter: Settings → Project → Python Interpreter
3. Add existing interpreter: `./venv/bin/python`
4. Mark `neural` as sources root

---

## Getting Help

- **Installation issues:** See [Troubleshooting](#troubleshooting) section
- **Training issues:** See [HEIMDALL_TRAINING.md](docs/HEIMDALL_TRAINING.md)
- **General questions:** Check [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
- **Dataset issues:** See [PERFECT_TRAINING_DATA.md](docs/PERFECT_TRAINING_DATA.md)

---

**Setup complete! 🎉 You're ready to train Heimdall.**
