# Next Steps After Training

Your model training completed successfully! Here's what to do next:

## Training Summary

- **Model**: `models/nornicdb-cypher-expert`
- **Final Loss**: 0.308
- **Training Time**: ~25 hours
- **Epochs**: 5.0
- **Model Type**: LoRA adapter (needs merging before GGUF export)

---

## Step 1: Optional Evaluation

If you have a validation set, you can evaluate the model:

```bash
cd neural
source venv/bin/activate  # If using venv

# Run evaluation (requires validation split in dataset)
python train.py \
    --preset qwen_0_5b \
    --dataset data/nornicdb_cypher.jsonl \
    --output_dir models/nornicdb-cypher-expert \
    --eval_only
```

**Note**: Your training used `--train-only` without a validation split, so evaluation may not be available. This is optional.

---

## Step 2: Export to GGUF Format

The model needs to be exported to GGUF format for use in NornicDB. This involves:
1. Merging LoRA adapters back into the base model
2. Converting to FP16 GGUF
3. Quantizing to a smaller format (optional but recommended)

### Option A: Using the Export Script (Recommended)

```bash
cd neural
source venv/bin/activate  # If using venv

# Check if llama.cpp is available
python3 -c "from export import GGUFConverter; c = GGUFConverter(); print('Ready:', c.check_dependencies())"

# If dependencies are missing, install them:
pip install llama-cpp-python
# OR clone llama.cpp:
# git clone https://github.com/ggerganov/llama.cpp
# cd llama.cpp && make

# Export with Q4_K_M quantization (recommended: good quality, ~1GB)
python export_to_gguf.py \
    --model_dir models/nornicdb-cypher-expert \
    --output nornicdb-cypher-expert-q4.gguf \
    --quantization Q4_K_M

# Alternative quantizations:
# Q5_K_M: Better quality, ~1.2GB
# Q8_0: Maximum quality, ~1.8GB
# F16: No quantization, largest size
```

### Option B: Using the Training Script

```bash
cd neural
source venv/bin/activate

python train_nornicdb_cypher.py \
    --export \
    --output-dir models/nornicdb-cypher-expert \
    --export-output nornicdb-cypher-expert-q4.gguf \
    --quantization Q4_K_M
```

### What Happens During Export

1. **LoRA Merge**: The adapter weights are merged back into the base model
2. **FP16 Conversion**: Model is converted to FP16 GGUF format
3. **Quantization**: Model is quantized to reduce size (Q4_K_M = 4-bit, ~75% size reduction)
4. **Output**: Final `.gguf` file ready for inference

---

## Step 3: Test the Model

### Quick Test with llama.cpp CLI

```bash
# If you have llama.cpp installed
./llama.cpp/main -m nornicdb-cypher-expert-q4.gguf \
    -p "Generate a Cypher query to find all Person nodes"

# Or test with Python
python3 << EOF
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel

# Load base model
base_model = "Qwen/qwen3-0.6b-Instruct"
tokenizer = AutoTokenizer.from_pretrained(base_model)
model = AutoModelForCausalLM.from_pretrained(base_model)

# Load LoRA adapters
model = PeftModel.from_pretrained(model, "models/qwen3-0.6b")

# Test generation
prompt = "Generate a Cypher query to find all Person nodes"
inputs = tokenizer(prompt, return_tensors="pt")
outputs = model.generate(**inputs, max_length=256)
print(tokenizer.decode(outputs[0]))
EOF
```

---

## Step 4: Deploy to NornicDB

### Copy Model to NornicDB Models Directory

```bash
# Create models directory if it doesn't exist
mkdir -p ../models

# Copy GGUF file
cp qwen3-0.6b.gguf ../models/

# Verify
ls -lh ../models/qwen3-0.6b.gguf
```

### Use in NornicDB Go Code

```go
package main

import (
    "context"
    "log"
    "github.com/your-org/nornicdb/pkg/localllm"
)

func main() {
    // Load the trained model
    opts := localllm.DefaultGenerationOptions(
        "../models/qwen3-0.6b.gguf",
    )
    
    model, err := localllm.LoadGenerationModel(opts)
    if err != nil {
        log.Fatal(err)
    }
    defer model.Close()
    
    // Generate Cypher query
    ctx := context.Background()
    prompt := "Find all Person nodes with age greater than 30"
    
    params := localllm.GenerationParams{
        MaxTokens: 256,
        Temperature: 0.7,
    }
    
    response, err := model.Generate(ctx, prompt, params)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Generated Cypher: %s", response.Text)
}
```

---

## Step 5: Integration Testing

Test the model with real NornicDB queries:

```bash
# Start NornicDB with your model
./nornicdb --model-path models/qwen3-0.6b.gguf

# Test Cypher generation via API
curl -X POST http://localhost:7474/db/cypher \
    -H "Content-Type: application/json" \
    -d '{
        "query": "Generate a query to find all users who created posts in the last week",
        "use_model": true
    }'
```

---

## Troubleshooting

### Export Fails: "llama.cpp not found"

```bash
# Option 1: Install llama-cpp-python (easier)
pip install llama-cpp-python

# Option 2: Clone and build llama.cpp
git clone https://github.com/ggerganov/llama.cpp
cd llama.cpp
make
export LLAMA_CPP_PATH=$(pwd)
```

### Export Fails: "LoRA merge failed"

The export script automatically detects and merges LoRA adapters. If merge fails:

1. Check that the base model is accessible:
   ```bash
   python3 -c "from transformers import AutoModelForCausalLM; AutoModelForCausalLM.from_pretrained('Qwen/qwen3-0.6b-Instruct')"
   ```

2. Manually merge first:
   ```bash
   python export_to_gguf.py \
       --model_dir models/nornicdb-cypher-expert \
       --base_model Qwen/Qwen3-0.6B-Instruct \
       --merge_only \
       --output models/nornicdb-cypher-expert-merged
   ```

### Model Too Large for Inference

Use a more aggressive quantization:

```bash
# Q3_K_M: Smaller, some quality loss
python export_to_gguf.py ... --quantization Q3_K_M

# Q2_K: Smallest, significant quality loss
python export_to_gguf.py ... --quantization Q2_K
```

---

## Performance Expectations

- **Model Size**: 
  - FP16: ~1.0 GB
  - Q4_K_M: ~250-300 MB
  - Q8_0: ~500 MB

- **Inference Speed** (on CPU):
  - ~5-10 tokens/second (depends on hardware)

- **Inference Speed** (on GPU):
  - ~50-100 tokens/second (depends on GPU)

- **Quality**: Q4_K_M maintains ~95% of FP16 quality with 75% size reduction

---

## Next: Continuous Improvement

1. **Collect feedback**: Test the model on real queries
2. **Fine-tune further**: Add more training data if needed
3. **Evaluate metrics**: Track query accuracy and user satisfaction
4. **Iterate**: Train new versions with improved datasets

---

## Quick Reference Commands

```bash
# Full export pipeline
cd neural
source venv/bin/activate
python export_to_gguf.py \
    --model_dir models/nornicdb-cypher-expert \
    --output nornicdb-cypher-expert-q4.gguf \
    --quantization Q4_K_M

# Copy to NornicDB
cp nornicdb-cypher-expert-q4.gguf ../models/

# Test
./llama.cpp/main -m ../models/nornicdb-cypher-expert-q4.gguf -p "Test prompt"
```

---

**Congratulations on completing training! 🎉**

Your model is ready for deployment. The next step is exporting to GGUF format so it can be used in NornicDB for Cypher query generation.
