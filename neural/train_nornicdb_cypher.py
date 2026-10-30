#!/usr/bin/env python3
"""
Train Qwen 0.5B Instruct on NornicDB Cypher queries and documentation.

This script:
1. Generates comprehensive Cypher training data
2. Optionally includes documentation examples
3. Trains Qwen 0.5B Instruct model
4. Exports to GGUF format

Usage:
    python train_nornicdb_cypher.py --generate-data --train --export
    python train_nornicdb_cypher.py --train-only --dataset data/nornicdb_cypher.jsonl
"""

import argparse
import sys
import logging
import subprocess
from pathlib import Path

# Check for required dependencies
try:
    import yaml
except ImportError:
    print("ERROR: Missing required dependency 'PyYAML'")
    print("\nPlease install dependencies:")
    print("  pip install -r requirements.txt")
    print("  or")
    print("  pip install PyYAML")
    sys.exit(1)

# Add neural directory to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from training import TrainingConfig, ModelPreset, NornicTrainer
    from training.trainer import estimate_memory_usage
except ImportError as e:
    print(f"ERROR: Failed to import training modules: {e}")
    print("\nPlease install dependencies:")
    print("  cd neural")
    print("  pip install -r requirements.txt")
    sys.exit(1)

logger = logging.getLogger(__name__)


def generate_training_data(output_path: str = "data/nornicdb_cypher.jsonl", count: int = 2000):
    """Generate Cypher training dataset."""
    print(f"\n{'='*60}")
    print("Step 1: Generating Cypher Training Data")
    print(f"{'='*60}\n")
    
    script_path = Path(__file__).parent / "scripts" / "generate_nornicdb_cypher_dataset.py"
    
    if not script_path.exists():
        logger.error(f"Dataset generator not found: {script_path}")
        return False
    
    cmd = [
        sys.executable,
        str(script_path),
        "--output", output_path,
        "--count", str(count),
        "--seed", "42"
    ]
    
    print(f"Running: {' '.join(cmd)}\n")
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode != 0:
        logger.error("Failed to generate training data")
        return False
    
    print(f"\n✓ Training data generated: {output_path}\n")
    return True


def train_model(
    dataset_path: str,
    output_dir: str = "models/nornicdb-cypher-expert",
    epochs: int = 5,
    batch_size: int = 8,
    gradient_accumulation: int = 4,
    learning_rate: float = 3e-4,
    use_qlora: bool = False,
    local_model_path: str = None,
):
    """Train Qwen 0.5B Instruct on Cypher dataset."""
    print(f"\n{'='*60}")
    print("Step 2: Training Qwen 0.5B Instruct")
    print(f"{'='*60}\n")
    
    # Disable SSL verification to avoid certificate issues
    import ssl
    import os
    ssl._create_default_https_context = ssl._create_unverified_context
    os.environ['CURL_CA_BUNDLE'] = ''
    os.environ['REQUESTS_CA_BUNDLE'] = ''
    os.environ['HF_HUB_DISABLE_SSL'] = '1'  # HuggingFace specific
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass
    try:
        import requests
        requests.packages.urllib3.disable_warnings()
    except (ImportError, AttributeError):
        pass
    print("⚠️  SSL verification disabled (ignoring certificate errors)\n")
    
    # Use Qwen 0.5B preset
    config = ModelPreset.qwen_0_5b()
    
    # Override with custom settings
    config.dataset_path = dataset_path
    config.output_dir = output_dir
    
    # Use local model if provided
    if local_model_path:
        if os.path.exists(local_model_path):
            config.base_model = local_model_path
            print(f"Using local model: {local_model_path}\n")
        else:
            print(f"⚠️  Warning: Local model path not found: {local_model_path}")
            print("   Falling back to HuggingFace download\n")
    config.num_train_epochs = epochs
    config.per_device_train_batch_size = batch_size
    config.gradient_accumulation_steps = gradient_accumulation
    config.learning_rate = learning_rate
    config.use_qlora = use_qlora
    
    # Optimize for Cypher query generation
    config.model_max_length = 2048  # Sufficient for most Cypher queries
    config.lora_r = 16
    config.lora_alpha = 32
    config.gradient_checkpointing = True
    
    # Print configuration
    print("Training Configuration:")
    print(f"  Base Model:      {config.base_model}")
    print(f"  Dataset:        {config.dataset_path}")
    print(f"  Output Dir:     {config.output_dir}")
    print(f"  Epochs:          {config.num_train_epochs}")
    print(f"  Batch Size:     {config.per_device_train_batch_size}")
    print(f"  Grad Accum:      {config.gradient_accumulation_steps}")
    print(f"  Effective Batch: {config.per_device_train_batch_size * config.gradient_accumulation_steps}")
    print(f"  Learning Rate:   {config.learning_rate}")
    print(f"  LoRA Rank:       {config.lora_r}")
    print(f"  QLoRA:           {config.use_qlora}")
    print(f"  Max Length:      {config.model_max_length}")
    print()
    
    # Estimate memory
    mem = estimate_memory_usage(config)
    print("Memory Usage Estimate:")
    print(f"  Total: {mem['total_estimated']:.2f} GB")
    if mem['total_estimated'] > 11:
        print("  ⚠️  WARNING: May exceed 11GB VRAM (2080 Ti)")
    else:
        print("  ✓ Should fit in 11GB VRAM (2080 Ti)")
    print()
    
    # Create trainer
    trainer = NornicTrainer(config)
    
    # Train
    print("Starting training...\n")
    result = trainer.train()
    
    # Print summary
    print(f"\n{'='*60}")
    print("Training Complete!")
    print(f"{'='*60}")
    print(f"Output Directory: {config.output_dir}")
    if result.metrics:
        print(f"Training Loss:    {result.metrics.get('train_loss', 'N/A')}")
        if 'eval_loss' in result.metrics:
            print(f"Validation Loss:  {result.metrics.get('eval_loss', 'N/A')}")
    print()
    
    return True


def export_to_gguf(
    model_dir: str,
    output_file: str = "nornicdb-cypher-expert-q4.gguf",
    quantization: str = "Q4_K_M"
):
    """Export trained model to GGUF format."""
    print(f"\n{'='*60}")
    print("Step 3: Exporting to GGUF")
    print(f"{'='*60}\n")
    
    export_script = Path(__file__).parent / "export_to_gguf.py"
    
    if not export_script.exists():
        logger.error(f"Export script not found: {export_script}")
        return False
    
    cmd = [
        sys.executable,
        str(export_script),
        "--model_dir", model_dir,
        "--output", output_file,
        "--quantization", quantization
    ]
    
    print(f"Running: {' '.join(cmd)}\n")
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode != 0:
        logger.error("Failed to export model")
        return False
    
    print(f"\n✓ Model exported: {output_file}\n")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Train Qwen 0.5B Instruct on NornicDB Cypher queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline: generate data, train, export
  python train_nornicdb_cypher.py --generate-data --train --export
  
  # Train only (data already generated)
  python train_nornicdb_cypher.py --train-only --dataset data/nornicdb_cypher.jsonl
  
  # Generate data only
  python train_nornicdb_cypher.py --generate-data --count 5000
  
  # Train with custom settings
  python train_nornicdb_cypher.py --train-only --dataset data/nornicdb_cypher.jsonl \\
      --epochs 10 --batch-size 4 --learning-rate 2e-4
        """
    )
    
    # Pipeline steps
    parser.add_argument("--generate-data", action="store_true",
                      help="Generate Cypher training dataset")
    parser.add_argument("--train", action="store_true",
                      help="Train the model")
    parser.add_argument("--train-only", action="store_true",
                      help="Train only (skip data generation)")
    parser.add_argument("--export", action="store_true",
                      help="Export trained model to GGUF")
    
    # Data generation
    parser.add_argument("--dataset", type=str, default="data/nornicdb_cypher.jsonl",
                      help="Path to training dataset")
    parser.add_argument("--data-count", type=int, default=2000,
                      help="Number of training examples to generate")
    
    # Training
    parser.add_argument("--output-dir", type=str, default="models/nornicdb-cypher-expert",
                      help="Output directory for trained model")
    parser.add_argument("--epochs", type=int, default=5,
                      help="Number of training epochs")
    parser.add_argument("--batch-size", type=int, default=8,
                      help="Training batch size")
    parser.add_argument("--gradient-accumulation", type=int, default=4,
                      help="Gradient accumulation steps")
    parser.add_argument("--learning-rate", type=float, default=3e-4,
                      help="Learning rate")
    parser.add_argument("--use-qlora", action="store_true",
                      help="Use QLoRA (4-bit quantization)")
    parser.add_argument("--local-model", type=str,
                      help="Path to local model directory (skip HuggingFace download)")
    
    # Export
    parser.add_argument("--export-output", type=str, default="nornicdb-cypher-expert-q4.gguf",
                      help="Output GGUF filename")
    parser.add_argument("--quantization", type=str, default="Q4_K_M",
                      choices=["Q2_K", "Q4_K_M", "Q5_K_M", "Q8_0", "F16"],
                      help="Quantization type")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        level=logging.INFO,
    )
    
    # Determine what to do
    generate_data = args.generate_data
    train = args.train or args.train_only
    export = args.export
    
    if not (generate_data or train or export):
        parser.print_help()
        print("\n⚠️  No action specified. Use --generate-data, --train, or --export")
        sys.exit(1)
    
    success = True
    
    # Step 1: Generate data
    if generate_data:
        success = generate_training_data(args.dataset, args.data_count)
        if not success:
            sys.exit(1)
    
    # Step 2: Train
    if train:
        if not Path(args.dataset).exists():
            logger.error(f"Dataset not found: {args.dataset}")
            logger.error("Run with --generate-data first, or specify --dataset")
            sys.exit(1)
        
        success = train_model(
            dataset_path=args.dataset,
            output_dir=args.output_dir,
            epochs=args.epochs,
            batch_size=args.batch_size,
            gradient_accumulation=args.gradient_accumulation,
            learning_rate=args.learning_rate,
            use_qlora=args.use_qlora,
            local_model_path=args.local_model,
        )
        if not success:
            sys.exit(1)
    
    # Step 3: Export
    if export:
        if not Path(args.output_dir).exists():
            logger.error(f"Model directory not found: {args.output_dir}")
            logger.error("Train the model first with --train")
            sys.exit(1)
        
        success = export_to_gguf(
            model_dir=args.output_dir,
            output_file=args.export_output,
            quantization=args.quantization
        )
        if not success:
            sys.exit(1)
    
    # Final summary
    if success:
        print(f"\n{'='*60}")
        print("✓ All steps completed successfully!")
        print(f"{'='*60}\n")
        
        if export:
            print("Next steps:")
            print(f"  1. Copy model to NornicDB: cp {args.export_output} ../models/")
            print(f"  2. Use in NornicDB: Load with localllm.LoadGenerationModel()")
            print(f"  3. Test Cypher generation with your trained model\n")


if __name__ == "__main__":
    main()
