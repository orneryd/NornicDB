# Neural Training System - Complete Guide

**Quick Navigation:**
- [Quick Start](#quick-start) - Get training in 5 minutes
- [Dataset Generators](#dataset-generators) - Generate domain-specific data
- [Training](#training) - Train your models
- [Export & Deploy](#export--deploy) - Convert to GGUF and use
- [Domain Guides](#domain-guides) - Specialized training strategies
- [Reference](#reference) - Detailed documentation

---

## Quick Start

### 1. Install Dependencies

```bash
cd neural/
pip install -r requirements.txt
```

### 2. Generate Dataset

Choose your domain:

```bash
# Cypher queries (graph database)
python scripts/generate_cypher_dataset.py --output data/cypher.jsonl

# Coding assistant
python scripts/generate_coding_dataset.py --language python --output data/coding.jsonl

# MTG judge
python scripts/generate_mtg_dataset.py --output data/mtg.jsonl

# Repository patterns (your codebase)
python scripts/extract_repo_patterns.py --repo . --language go --output data/repo.jsonl
```

### 3. Train Model

```bash
# For 2080 Ti (11GB VRAM)
python train.py --preset qwen_1_5b --dataset data/cypher.jsonl --output_dir models/my-model

# For Apple Silicon
python train.py --preset qwen_1_5b --dataset data/cypher.jsonl --output_dir models/my-model --use_metal
```

### 4. Export to GGUF

```bash
python export_to_gguf.py --model_dir models/my-model --output my-model-q4.gguf
```

### 5. Use in NornicDB

```bash
# Copy to models directory
cp my-model-q4.gguf /data/models/

# Use with inference engine
./nornicdb --model /data/models/my-model-q4.gguf
```

---

## Dataset Generators

### Overview

| Generator | Purpose | Languages | Output Examples |
|-----------|---------|-----------|-----------------|
| `generate_cypher_dataset.py` | Cypher query generation | Cypher | 2000+ |
| `extract_repo_patterns.py` | Code pattern extraction | Go, Python, JS, Java | Variable |
| `generate_mtg_dataset.py` | MTG judge training | English | 1000+ |
| `generate_coding_dataset.py` | General coding tasks | Python, Go, JS, Java | 1000+ |

### Cypher Dataset Generator

**Purpose:** Train models to generate Cypher queries from natural language

**Coverage:**
- ✅ All MATCH patterns (nodes, relationships, properties)
- ✅ All functions (aggregation, scalar, list, string, math)
- ✅ All clauses (WHERE, ORDER BY, LIMIT, WITH, etc.)
- ✅ Complex multi-clause queries
- ✅ Systematic syntax enumeration (18% of dataset)

**Usage:**
```bash
python scripts/generate_cypher_dataset.py \
    --output data/cypher_queries.jsonl \
    --num-examples 2000
```

**Example output:**
```json
{
  "instruction": "Write a Cypher query",
  "input": "Find all users who have purchased products in the last 30 days",
  "output": "MATCH (u:User)-[:PURCHASED]->(p:Product) WHERE p.date > date() - duration('P30D') RETURN u.name, p.title",
  "category": "cypher_query"
}
```

### Repository Pattern Extractor

**Purpose:** Learn from existing codebases

**Extracts:**
- Function implementations
- Type definitions (structs, interfaces, classes)
- Test patterns
- API usage examples
- Error handling patterns
- Documentation

**Usage:**
```bash
python scripts/extract_repo_patterns.py \
    --repo /path/to/repo \
    --language go \
    --output data/repo_patterns.jsonl
```

**Use cases:**
- Train on NornicDB codebase
- Learn project-specific patterns
- Generate context-aware code
- Understand architectural decisions

### MTG Dataset Generator

**Purpose:** Train MTG judge models

**Covers:**
- Comprehensive Rules (all 9 sections)
- Stack interactions (LIFO resolution)
- Tournament rulings (REL policies)
- Card interactions
- Keyword abilities
- Complex scenarios

**Usage:**
```bash
python scripts/generate_mtg_dataset.py \
    --output data/mtg_judge.jsonl \
    --num-examples 1000 \
    --include-advanced
```

**Example output:**
```json
{
  "instruction": "Resolve this MTG stack interaction",
  "input": "I cast Lightning Bolt targeting your creature. You cast Counterspell. What happens?",
  "output": "Counterspell resolves first (LIFO). It counters Lightning Bolt. Lightning Bolt goes to graveyard without resolving. Creature survives.",
  "category": "stack_interaction"
}
```

### Coding Assistant Generator

**Purpose:** General programming assistance

**Distribution:**
- 40% Code generation (algorithms, data structures)
- 30% Bug fixing (identify and fix bugs)
- 20% Code understanding (explain implementations)
- 10% Test generation (write unit tests)

**Usage:**
```bash
python scripts/generate_coding_dataset.py \
    --language python \
    --output data/coding.jsonl \
    --num-examples 1000
```

**Supports:**
- Python (functions, classes, comprehensions)
- Go (functions, structs, interfaces, goroutines)
- JavaScript (ES6+, async/await)
- Java (OOP, generics)

---

## Training

### Model Presets

Choose based on your hardware:

| Preset | Parameters | VRAM (QLoRA) | VRAM (LoRA) | Best For |
|--------|------------|--------------|-------------|----------|
| `qwen_0_5b` | 0.5B | 3-4 GB | 6-7 GB | Testing, rapid iteration |
| `tinyllama_1b` | 1.1B | 4-5 GB | 7-8 GB | Lightweight production |
| `qwen_1_5b` | 1.5B | 5-6 GB | 9-10 GB | **2080 Ti sweet spot** |
| `phi3_mini` | 3.8B | 8-9 GB | 14-15 GB | Maximum quality (11GB limit) |
| `llama3_1b` | 1B | 4-5 GB | 7-8 GB | Instruction-following |

### Training Commands

#### NVIDIA GPU (2080 Ti)

```bash
# QLoRA (4-bit) - Most memory efficient
python train.py \
    --preset qwen_1_5b \
    --dataset data/your_data.jsonl \
    --output_dir models/my-model \
    --use_qlora \
    --batch_size 4 \
    --gradient_accumulation 4 \
    --epochs 5

# LoRA (16-bit) - Faster training
python train.py \
    --preset qwen_0_5b \
    --dataset data/your_data.jsonl \
    --output_dir models/my-model \
    --batch_size 8 \
    --gradient_accumulation 2 \
    --epochs 5
```

#### Apple Silicon (Metal)

```bash
python train.py \
    --preset qwen_1_5b \
    --dataset data/your_data.jsonl \
    --output_dir models/my-model \
    --use_metal \
    --batch_size 4 \
    --epochs 5
```

#### Memory Estimation

```bash
# Check memory requirements before training
python train.py --preset qwen_1_5b --estimate_memory
```

### Training Tips

**Batch Size:**
- Start with 4, increase if memory allows
- Use gradient accumulation if OOM: `--gradient_accumulation 4`
- Effective batch size = batch_size × gradient_accumulation

**Learning Rate:**
- Default: 2e-4 (good for most cases)
- Increase for small datasets: 3e-4
- Decrease for large datasets: 1e-4

**Epochs:**
- Small datasets (< 500): 8-10 epochs
- Medium datasets (500-2000): 5-8 epochs
- Large datasets (> 2000): 3-5 epochs

**Overfitting prevention:**
- Monitor validation loss
- Stop if val_loss stops decreasing
- Use more diverse data

---

## Export & Deploy

### Export to GGUF

```bash
# Q4_K_M quantization (recommended - 4-bit, good quality)
python export_to_gguf.py \
    --model_dir models/my-model \
    --output my-model-q4.gguf \
    --quant_type Q4_K_M

# Q5_K_M (better quality, slightly larger)
python export_to_gguf.py \
    --model_dir models/my-model \
    --output my-model-q5.gguf \
    --quant_type Q5_K_M

# Q8_0 (maximum quality, 2x larger)
python export_to_gguf.py \
    --model_dir models/my-model \
    --output my-model-q8.gguf \
    --quant_type Q8_0
```

### Quantization Types

| Type | Bits | Size | Quality | Use Case |
|------|------|------|---------|----------|
| Q2_K | 2-bit | Smallest | Lowest | Extreme compression |
| Q4_K_M | 4-bit | Small | Good | **Recommended** |
| Q5_K_M | 5-bit | Medium | Better | Quality focus |
| Q8_0 | 8-bit | Large | Best | Maximum quality |
| F16 | 16-bit | 2x Q8 | Perfect | Development |

### Deploy to NornicDB

```bash
# Copy GGUF file
cp my-model-q4.gguf /data/models/

# Update config
# Edit nornicdb.yaml:
# llm:
#   model_path: /data/models/my-model-q4.gguf

# Start NornicDB
./nornicdb --config nornicdb.yaml
```

---

## Domain Guides

Detailed training strategies for specific domains:

### 1. Cypher Query Expert

**Goal:** Natural language → Cypher query generation

**Strategy:**
1. Generate base dataset: `generate_cypher_dataset.py`
2. Add real-world queries from your schema
3. Include edge cases (empty results, complex joins)
4. Mix with 20% general English examples

**Training:**
```bash
python train.py --preset qwen_1_5b --dataset data/cypher.jsonl --epochs 5
```

**Validation:**
- Test on holdout queries
- Check syntax validity
- Verify semantic correctness

**See:** [`docs/DOMAIN_TRAINING_GUIDES.md#cypher-expert`](docs/DOMAIN_TRAINING_GUIDES.md)

### 2. Repository Expert

**Goal:** Understand and generate code for specific project

**Strategy:**
1. Extract patterns: `extract_repo_patterns.py --repo .`
2. Add architecture documentation
3. Include common patterns and idioms
4. Mix with 30% general coding examples

**Training:**
```bash
python train.py --preset qwen_1_5b --dataset data/repo.jsonl --epochs 8
```

**Use cases:**
- Code completion
- API usage examples
- Bug fixing in project context
- Explaining architecture

**See:** [`docs/DOMAIN_TRAINING_GUIDES.md#repository-expert`](docs/DOMAIN_TRAINING_GUIDES.md)

### 3. MTG Judge

**Goal:** Rules enforcement and tournament policy

**Strategy:**
1. Generate base: `generate_mtg_dataset.py`
2. Parse comprehensive rules document
3. Add tournament policy (IPG, MTR)
4. Include real judge scenarios

**Training:**
```bash
python train.py --preset phi3_mini --dataset data/mtg.jsonl --epochs 6
```

**See:** [`docs/DOMAIN_TRAINING_GUIDES.md#mtg-judge`](docs/DOMAIN_TRAINING_GUIDES.md)

### 4. Coding Assistant

**Goal:** General programming help

**Strategy:**
1. Generate base: `generate_coding_dataset.py`
2. Add real bugs from issue trackers
3. Include algorithms and data structures
4. Mix multiple languages

**Training:**
```bash
python train.py --preset qwen_1_5b --dataset data/coding.jsonl --epochs 5
```

**See:** [`docs/DOMAIN_TRAINING_GUIDES.md#agentic-coding`](docs/DOMAIN_TRAINING_GUIDES.md)

---

## Reference

### Documentation

- **[README.md](README.md)** - Main documentation
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Quick command lookup
- **[docs/PERFECT_TRAINING_DATA.md](docs/PERFECT_TRAINING_DATA.md)** - Data quality guidelines
- **[docs/DOMAIN_TRAINING_GUIDES.md](docs/DOMAIN_TRAINING_GUIDES.md)** - Domain-specific strategies

### Scripts

- **`train.py`** - Main training script
- **`export_to_gguf.py`** - Model export and quantization
- **`scripts/generate_cypher_dataset.py`** - Cypher training data
- **`scripts/extract_repo_patterns.py`** - Repository analysis
- **`scripts/generate_mtg_dataset.py`** - MTG judge training
- **`scripts/generate_coding_dataset.py`** - Coding assistance
- **`scripts/validate_dataset.py`** - Dataset validation

### Hardware Support

| Hardware | Status | Notes |
|----------|--------|-------|
| NVIDIA GPU (CUDA) | ✅ Full support | Flash Attention 2, optimal performance |
| Apple Silicon (Metal) | ✅ Full support | MPS backend, native acceleration |
| CPU | ✅ Fallback | Slow but functional |

---

## Troubleshooting

### Out of Memory (OOM)

```bash
# Use QLoRA
python train.py --preset qwen_0_5b --use_qlora

# Reduce batch size
python train.py --batch_size 1 --gradient_accumulation 16

# Use smaller model
python train.py --preset qwen_0_5b
```

### Slow Training

```bash
# Use LoRA instead of QLoRA (if you have memory)
python train.py --preset qwen_1_5b  # No --use_qlora

# Increase batch size
python train.py --batch_size 8

# Use GPU (not CPU)
python train.py  # Auto-detects GPU
```

### Metal/MPS Errors (Apple Silicon)

```bash
# Enable fallback to CPU for unsupported ops
export PYTORCH_ENABLE_MPS_FALLBACK=1
python train.py --use_metal
```

### Export Fails

```bash
# Install llama.cpp
git clone https://github.com/ggerganov/llama.cpp
cd llama.cpp && make

# Merge LoRA adapters first
python export_to_gguf.py --model_dir models/my-model --merge_adapters
```

---

## Performance Tips

### Training Speed

1. **Use LoRA** instead of QLoRA (if memory allows)
2. **Increase batch size** to saturate GPU
3. **Use gradient checkpointing** for memory vs speed trade-off
4. **Enable Flash Attention 2** (NVIDIA only)

### Model Quality

1. **More diverse data** beats more epochs
2. **Mix domain data with general examples** (70/30)
3. **Validate on holdout set** to prevent overfitting
4. **Use larger models** if memory allows (phi3_mini > qwen_1_5b > qwen_0_5b)

### Inference Speed

1. **Use Q4_K_M quantization** for best size/quality ratio
2. **Run on GPU** with llama.cpp CUDA backend
3. **Reduce context length** if possible
4. **Batch requests** when possible

---

## Next Steps

1. **Generate your dataset:** Choose appropriate generator
2. **Validate quality:** Run `validate_dataset.py`
3. **Train model:** Use appropriate preset for hardware
4. **Export to GGUF:** Quantize for production
5. **Deploy:** Use in NornicDB or other applications

**Need help?** Check the detailed guides:
- Quality: [`docs/PERFECT_TRAINING_DATA.md`](docs/PERFECT_TRAINING_DATA.md)
- Domains: [`docs/DOMAIN_TRAINING_GUIDES.md`](docs/DOMAIN_TRAINING_GUIDES.md)
- Quick ref: [`QUICK_REFERENCE.md`](QUICK_REFERENCE.md)
