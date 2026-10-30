#!/usr/bin/env python3
"""
Main training script for NornicDB neural models.
Fine-tune models on custom datasets and export to GGUF format.

Example usage:
    # Train Heimdall (recommended for NornicDB)
    python train.py --preset heimdall --dataset data/heimdall_training.jsonl --epochs 10
    
    # Train Heimdall MIT variant (higher quality, MIT license)
    python train.py --preset heimdall_mit --dataset data/heimdall_training.jsonl --epochs 10
    
    # Basic training with default preset
    python train.py --dataset data/training.jsonl --output_dir models/custom
    
    # Custom configuration from YAML
    python train.py --config config/my_config.yaml --dataset data/training.jsonl
    
    # Test on small dataset
    python train.py --preset qwen_0_5b --dataset data/training.jsonl --max_samples 100
"""

import argparse
import sys
import logging
from pathlib import Path

import torch

from training import TrainingConfig, ModelPreset, NornicTrainer
from training.trainer import estimate_memory_usage

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Train custom LLM for NornicDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    # Configuration
    config_group = parser.add_mutually_exclusive_group()
    config_group.add_argument(
        "--config",
        type=str,
        help="Path to YAML configuration file"
    )
    config_group.add_argument(
        "--preset",
        type=str,
        choices=["tinyllama_1b", "qwen_0_5b", "qwen_1_5b", "phi3_mini", "phi3_5_mini", 
                 "heimdall", "heimdall_mit"],
        default="qwen_1_5b",
        help="Use preset configuration (default: qwen_1_5b, recommended: heimdall for NornicDB)"
    )
    
    # Dataset
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="Path to training dataset (JSONL format)"
    )
    parser.add_argument(
        "--max_samples",
        type=int,
        help="Limit number of training samples (for testing)"
    )
    
    # Output
    parser.add_argument(
        "--output_dir",
        type=str,
        default="models/trained",
        help="Output directory for trained model"
    )
    
    # Training overrides
    parser.add_argument(
        "--base_model",
        type=str,
        help="Override base model name"
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        help="Override training batch size"
    )
    parser.add_argument(
        "--gradient_accumulation",
        type=int,
        help="Override gradient accumulation steps"
    )
    parser.add_argument(
        "--epochs",
        type=int,
        help="Override number of training epochs"
    )
    parser.add_argument(
        "--learning_rate",
        type=float,
        help="Override learning rate"
    )
    
    # Optimization
    parser.add_argument(
        "--use_qlora",
        action="store_true",
        help="Enable QLoRA (4-bit quantized training)"
    )
    parser.add_argument(
        "--no_flash_attention",
        action="store_true",
        help="Disable Flash Attention 2"
    )
    
    # Misc
    parser.add_argument(
        "--estimate_memory",
        action="store_true",
        help="Estimate VRAM usage and exit"
    )
    parser.add_argument(
        "--eval_only",
        action="store_true",
        help="Only run evaluation (requires trained model)"
    )
    
    return parser.parse_args()


def load_config(args) -> TrainingConfig:
    """Load configuration from args."""
    if args.config:
        # Load from YAML
        logger.info(f"Loading config from {args.config}")
        config = TrainingConfig.from_yaml(args.config)
    else:
        # Use preset
        logger.info(f"Using preset: {args.preset}")
        preset_func = getattr(ModelPreset, args.preset)
        config = preset_func()
    
    # Apply overrides
    config.dataset_path = args.dataset
    config.output_dir = args.output_dir
    
    if args.max_samples:
        config.max_samples = args.max_samples
    if args.base_model:
        config.base_model = args.base_model
    if args.batch_size:
        config.per_device_train_batch_size = args.batch_size
    if args.gradient_accumulation:
        config.gradient_accumulation_steps = args.gradient_accumulation
    if args.epochs:
        config.num_train_epochs = args.epochs
    if args.learning_rate:
        config.learning_rate = args.learning_rate
    if args.use_qlora:
        config.use_qlora = True
    if args.no_flash_attention:
        config.use_flash_attention_2 = False
    
    return config


def print_config(config: TrainingConfig):
    """Print training configuration."""
    print("\n" + "="*60)
    print("Training Configuration")
    print("="*60)
    print(f"Base Model:           {config.base_model}")
    print(f"Dataset:              {config.dataset_path}")
    print(f"Output Directory:     {config.output_dir}")
    print(f"Max Sequence Length:  {config.model_max_length}")
    print()
    print(f"Epochs:               {config.num_train_epochs}")
    print(f"Batch Size:           {config.per_device_train_batch_size}")
    print(f"Gradient Accumulation: {config.gradient_accumulation_steps}")
    print(f"Effective Batch Size: {config.per_device_train_batch_size * config.gradient_accumulation_steps}")
    print(f"Learning Rate:        {config.learning_rate}")
    print()
    print(f"LoRA Enabled:         {config.use_lora}")
    if config.use_lora:
        print(f"  LoRA Rank:          {config.lora_r}")
        print(f"  LoRA Alpha:         {config.lora_alpha}")
    print(f"QLoRA Enabled:        {config.use_qlora}")
    print(f"Gradient Checkpoint:  {config.gradient_checkpointing}")
    print(f"Flash Attention 2:    {config.use_flash_attention_2}")
    print(f"Mixed Precision:      {'FP16' if config.fp16 else 'BF16' if config.bf16 else 'FP32'}")
    print("="*60 + "\n")


def check_dataset(path: str):
    """Verify dataset exists and is valid."""
    if not Path(path).exists():
        logger.error(f"Dataset not found: {path}")
        sys.exit(1)
    
    # Check format
    import json
    try:
        with open(path, 'r') as f:
            first_line = f.readline()
            json.loads(first_line)
        logger.info("✓ Dataset format valid")
    except Exception as e:
        logger.error(f"Invalid dataset format: {e}")
        logger.error("Expected JSONL format (one JSON object per line)")
        sys.exit(1)


def main():
    """Main training function."""
    # Setup logging
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        level=logging.INFO,
    )
    
    # Parse arguments
    args = parse_args()
    
    # Load configuration
    config = load_config(args)
    
    # Check GPU (CUDA for NVIDIA, MPS for Apple Silicon)
    has_cuda = torch.cuda.is_available()
    has_mps = torch.backends.mps.is_available() if hasattr(torch.backends, 'mps') else False
    
    if has_cuda:
        logger.info("✅ CUDA GPU detected - using NVIDIA acceleration")
    elif has_mps:
        logger.info("✅ MPS (Metal) detected - using Apple Silicon acceleration")
    else:
        logger.warning("⚠️  No GPU detected! Training will be very slow.")
        response = input("Continue anyway? [y/N]: ")
        if response.lower() != 'y':
            sys.exit(0)
    
    # Estimate memory if requested
    if args.estimate_memory:
        print_config(config)
        mem = estimate_memory_usage(config)
        print("Memory Usage Estimate:")
        print(f"  Base Model:      {mem['base_model']:.2f} GB")
        print(f"  LoRA Adapters:   {mem['lora_adapters']:.2f} GB")
        print(f"  Optimizer:       {mem['optimizer']:.2f} GB")
        print(f"  Gradients:       {mem['gradients']:.2f} GB")
        print(f"  Activations:     {mem['activations']:.2f} GB")
        print(f"  Total Estimate:  {mem['total_estimated']:.2f} GB")
        print()
        
        if mem['total_estimated'] > 11:
            print("⚠️  WARNING: Estimated usage exceeds 2080 Ti VRAM (11GB)")
            print("   Consider:")
            print("   - Reducing batch size (--batch_size)")
            print("   - Enabling QLoRA (--use_qlora)")
            print("   - Using smaller model")
        else:
            print("✓ Configuration should fit in 2080 Ti (11GB VRAM)")
        
        sys.exit(0)
    
    # Verify dataset
    check_dataset(config.dataset_path)
    
    # Print configuration
    print_config(config)
    
    # Create trainer
    trainer = NornicTrainer(config)
    
    # Run training or evaluation
    if args.eval_only:
        logger.info("Running evaluation only")
        metrics = trainer.evaluate()
        print("\nEvaluation Results:")
        for key, value in metrics.items():
            print(f"  {key}: {value:.4f}")
    else:
        # Train
        result = trainer.train()
        
        # Print summary
        print("\n" + "="*60)
        print("Training Complete!")
        print("="*60)
        print(f"Output Directory: {config.output_dir}")
        print(f"Training Loss:    {result.metrics.get('train_loss', 'N/A')}")
        print()
        print("Next steps:")
        print(f"  1. Export to GGUF: python export_to_gguf.py --model_dir {config.output_dir}")
        print(f"  2. Copy to models: cp output.gguf /data/models/")
        print(f"  3. Use in NornicDB: Load with localllm.LoadGenerationModel()")
        print("="*60 + "\n")


if __name__ == "__main__":
    main()
