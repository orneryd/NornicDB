# Quick Reference Guide - Neural Training System

**Fast lookup for common tasks**

---

## 🚀 Quick Start

### Train on 2080 Ti (NVIDIA)
```bash
python train.py --preset qwen_1_5b --dataset data/training.jsonl --output_dir models/trained
```

### Train on Apple Silicon (M1/M2/M3)
```bash
# Metal acceleration is automatic
python train.py --preset qwen_1_5b --dataset data/training.jsonl --output_dir models/trained
```

---

## 📊 Dataset Generation

### Cypher Queries (Recommended for NornicDB)
```bash
# Generate 2000 examples
python scripts/generate_cypher_dataset.py --count 2000 --output data/cypher.jsonl

# Validate
python scripts/validate_dataset.py data/cypher.jsonl --check-cypher

# Train
python train.py --preset qwen_1_5b --dataset data/cypher.jsonl --output_dir models/cypher-expert
```

### Custom Dataset Format
```jsonl
{"instruction": "Task description", "input": "Input data", "output": "Expected output"}
{"instruction": "Another task", "input": "More data", "output": "Another output"}
```

---

## 🎛️ Model Presets

| Preset | Size | VRAM | Best For |
|--------|------|------|----------|
| `qwen_0_5b` | 500M | 4GB | Ultra-fast, simple tasks |
| `tinyllama_1b` | 1.1B | 6GB | Quick experiments |
| `qwen_1_5b` | 1.5B | 8GB | **Recommended** - Best balance |
| `llama3_1b` | 1B | 6GB | Meta's efficient model |
| `phi3_mini` | 3.8B | 10GB | Highest quality (requires QLoRA) |

### Using Presets
```bash
python train.py --preset qwen_1_5b --dataset data/training.jsonl
```

---

## ⚙️ Common Training Options

### Basic Training
```bash
python train.py \
  --dataset data/training.jsonl \
  --output_dir models/trained \
  --epochs 3
```

### Adjust Memory Usage
```bash
# Reduce memory (if OOM)
python train.py \
  --dataset data/training.jsonl \
  --batch_size 2 \
  --gradient_accumulation 16 \
  --use_qlora  # 4-bit training

# Increase speed (if memory available)
python train.py \
  --dataset data/training.jsonl \
  --batch_size 8 \
  --gradient_accumulation 4
```

### Estimate Memory Before Training
```bash
python train.py --preset phi3_mini --dataset data/training.jsonl --estimate_memory
```

---

## 📤 Export to GGUF

### Standard Export (Q4_K_M)
```bash
python export_to_gguf.py \
  --model_dir models/trained \
  --output model-q4.gguf
```

### Different Quantization
```bash
# Higher quality (larger file)
python export_to_gguf.py --model_dir models/trained --output model-q8.gguf --quantization Q8_0

# Smaller file (lower quality)
python export_to_gguf.py --model_dir models/trained --output model-q4s.gguf --quantization Q4_K_S

# No quantization (FP16)
python export_to_gguf.py --model_dir models/trained --output model-fp16.gguf --no_quantize
```

---

## 🔍 Quality Assurance

### Validate Dataset
```bash
# Basic validation
python scripts/validate_dataset.py data/training.jsonl

# With Cypher syntax checking
python scripts/validate_dataset.py data/cypher.jsonl --check-cypher
```

### Check Training Progress
```bash
# View TensorBoard logs
tensorboard --logdir models/trained/runs

# Check latest checkpoint
ls -lh models/trained/checkpoint-*
```

---

## 🐛 Troubleshooting

### Out of Memory (OOM)

**Solutions:**
```bash
# 1. Reduce batch size
python train.py --dataset data/training.jsonl --batch_size 1

# 2. Enable QLoRA (4-bit)
python train.py --dataset data/training.jsonl --use_qlora

# 3. Use smaller model
python train.py --preset qwen_0_5b --dataset data/training.jsonl

# 4. Increase gradient accumulation
python train.py --dataset data/training.jsonl --batch_size 2 --gradient_accumulation 16
```

### Slow Training

**Solutions:**
```bash
# 1. Check Flash Attention is enabled (auto on CUDA)
# 2. Reduce LoRA rank
python train.py --dataset data/training.jsonl --config config/custom.yaml
# Edit config: lora_r: 8 (default 16)

# 3. Use smaller max_length
# Edit config: model_max_length: 512 (default 2048)

# 4. Reduce dataset size for testing
python train.py --dataset data/training.jsonl --max_samples 100
```

### Metal (MPS) Errors on macOS

**Note**: Some operations fall back to CPU automatically
```bash
# Check PyTorch MPS support
python -c "import torch; print('MPS Available:', torch.backends.mps.is_available())"

# Disable Flash Attention on Metal
python train.py --dataset data/training.jsonl --no_flash_attention
```

### Export Fails

**Solutions:**
```bash
# 1. Install llama.cpp Python bindings
pip install llama-cpp-python

# 2. Or clone llama.cpp
git clone https://github.com/ggerganov/llama.cpp
cd llama.cpp && make

# 3. Specify llama.cpp path
python export_to_gguf.py --model_dir models/trained --output model.gguf --llama_cpp_path /path/to/llama.cpp
```

---

## 📈 Performance Monitoring

### Hardware Utilization

**NVIDIA GPU:**
```bash
# Monitor in real-time
watch -n 1 nvidia-smi
```

**Apple Silicon:**
```bash
# Check activity
sudo powermetrics --samplers gpu_power -i 1000 -n 1
```

### Training Metrics
```bash
# View loss curves
tensorboard --logdir models/trained

# Check logs
tail -f models/trained/trainer_state.json
```

---

## 🎯 Best Practices

### 1. Start Small
```bash
# Test with 100 examples first
python train.py --dataset data/training.jsonl --max_samples 100 --epochs 1
```

### 2. Validate Data
```bash
# Always validate before training
python scripts/validate_dataset.py data/training.jsonl
```

### 3. Use Presets
```bash
# Don't tune hyperparameters initially
python train.py --preset qwen_1_5b --dataset data/training.jsonl
```

### 4. Monitor Training
```bash
# Watch loss decrease
tensorboard --logdir models/trained
```

### 5. Evaluate
```bash
# Test on held-out examples manually
# Or use eval_only mode
python train.py --dataset data/training.jsonl --output_dir models/trained --eval_only
```

---

## 📚 Full Documentation

- **Main README**: `README.md`
- **Training Data Guide**: `docs/PERFECT_TRAINING_DATA.md`
- **Config Reference**: `training/config.py`
- **Example Datasets**: `data/examples/`

---

## 💡 Common Workflows

### Cypher Query Expert
```bash
python scripts/generate_cypher_dataset.py --count 2000 --output data/cypher.jsonl
python scripts/validate_dataset.py data/cypher.jsonl --check-cypher
python train.py --preset qwen_1_5b --dataset data/cypher.jsonl --output_dir models/cypher-expert --epochs 5
python export_to_gguf.py --model_dir models/cypher-expert --output cypher-expert-q4.gguf
```

### TLP Classifier
```bash
# Use provided example + add your own
cp data/examples/tlp_evaluation.jsonl data/tlp_full.jsonl
# Add more examples to data/tlp_full.jsonl
python train.py --preset tinyllama_1b --dataset data/tlp_full.jsonl --output_dir models/tlp
python export_to_gguf.py --model_dir models/tlp --output tlp-q4.gguf
```

### Custom Task
```bash
# 1. Create dataset (see docs/PERFECT_TRAINING_DATA.md)
# 2. Validate
python scripts/validate_dataset.py data/custom.jsonl
# 3. Train
python train.py --preset qwen_1_5b --dataset data/custom.jsonl --output_dir models/custom
# 4. Export
python export_to_gguf.py --model_dir models/custom --output custom-q4.gguf
# 5. Use in NornicDB
cp custom-q4.gguf /data/models/
```
