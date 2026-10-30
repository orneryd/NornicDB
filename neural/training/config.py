"""
Training configuration for neural models.
Optimized for NVIDIA 2080 Ti (11GB VRAM).
"""

from dataclasses import dataclass, field
from typing import List, Optional
import yaml


@dataclass
class TrainingConfig:
    """Configuration for model training with memory optimization."""
    
    # Model Configuration
    # Default: Apache 2.0 licensed model (permissive, commercial use OK)
    base_model: str = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"  # Apache 2.0
    model_max_length: int = 2048
    
    # Model Size (for training from scratch, optional)
    # If specified, trains a new model instead of fine-tuning
    hidden_size: Optional[int] = None  # e.g., 768, 1024, 2048
    num_hidden_layers: Optional[int] = None  # e.g., 12, 24, 32
    num_attention_heads: Optional[int] = None  # e.g., 12, 16, 32
    intermediate_size: Optional[int] = None  # FFN size, typically 4*hidden_size
    vocab_size: Optional[int] = None  # Vocabulary size
    train_from_scratch: bool = False  # Train new model vs fine-tune
    
    # LoRA Configuration (Parameter-Efficient Fine-Tuning)
    use_lora: bool = True
    lora_r: int = 16  # LoRA rank (lower = less memory, faster training)
    lora_alpha: int = 32  # LoRA scaling factor
    lora_dropout: float = 0.05
    target_modules: List[str] = field(default_factory=lambda: [
        "q_proj", "v_proj", "k_proj", "o_proj",  # Attention layers
        "gate_proj", "up_proj", "down_proj"  # MLP layers
    ])
    
    # QLoRA (4-bit quantization for training)
    use_qlora: bool = False  # Enable for >3B models on 2080 Ti
    bnb_4bit_compute_dtype: str = "float16"
    bnb_4bit_quant_type: str = "nf4"  # Normal Float 4-bit
    
    # Training Hyperparameters
    learning_rate: float = 2e-4
    num_train_epochs: int = 3
    per_device_train_batch_size: int = 4  # Adjust based on model size
    per_device_eval_batch_size: int = 4
    gradient_accumulation_steps: int = 8  # Effective batch = 4*8 = 32
    
    # Optimization
    optim: str = "adamw_torch"
    weight_decay: float = 0.01
    max_grad_norm: float = 1.0
    warmup_steps: int = 100
    
    # Memory Optimization
    gradient_checkpointing: bool = True  # Trade compute for memory
    fp16: bool = True  # Mixed precision (use bf16 for Ampere+ GPUs)
    bf16: bool = False  # Use this instead of fp16 on RTX 30xx+
    
    # Flash Attention (major speedup if available)
    use_flash_attention_2: bool = True
    
    # Hardware Acceleration
    use_metal: bool = True  # Use Metal on Apple Silicon (auto-detected)
    use_mps: bool = True  # Use MPS backend on macOS (auto-detected)
    
    # Data Configuration
    dataset_path: str = "data/training.jsonl"
    validation_split: float = 0.1
    max_samples: Optional[int] = None  # Limit dataset size for testing
    
    # Output Configuration
    output_dir: str = "models/trained"
    logging_steps: int = 5  # Log every 5 steps for frequent progress updates
    save_steps: int = 500
    eval_steps: int = 500
    save_total_limit: int = 3  # Keep only 3 best checkpoints
    
    # Evaluation
    evaluation_strategy: str = "steps"
    load_best_model_at_end: bool = True
    metric_for_best_model: str = "eval_loss"
    
    # Hardware
    dataloader_num_workers: int = 4
    ddp_find_unused_parameters: bool = False
    
    # Misc
    seed: int = 42
    report_to: List[str] = field(default_factory=lambda: ["tensorboard"])
    
    def to_yaml(self, path: str):
        """Save configuration to YAML file."""
        with open(path, 'w') as f:
            yaml.dump(self.__dict__, f, default_flow_style=False)
    
    @classmethod
    def from_yaml(cls, path: str):
        """Load configuration from YAML file."""
        with open(path, 'r') as f:
            config_dict = yaml.safe_load(f)
        return cls(**config_dict)


@dataclass
class ModelPreset:
    """Preset configurations for common model sizes."""
    
    @staticmethod
    def tiny_llama_1b():
        """TinyLlama 1.1B - Fastest training, good for experiments.
        
        License: Apache 2.0 (permissive, commercial use OK)
        """
        return TrainingConfig(
            base_model="TinyLlama/TinyLlama-1.1B-Chat-v1.0",
            per_device_train_batch_size=8,
            gradient_accumulation_steps=4,
            lora_r=16,
        )
    
    @staticmethod
    def qwen_0_5b():
        """Qwen2.5 0.5B - Ultra efficient, specific tasks.
        
        License: Apache 2.0 (permissive, commercial use OK)
        """
        return TrainingConfig(
            base_model="Qwen/qwen3-0.6b-Instruct",
            per_device_train_batch_size=16,
            gradient_accumulation_steps=2,
            lora_r=8,
        )
    
    @staticmethod
    def qwen_1_5b():
        """Qwen2.5 1.5B - Balanced quality and efficiency.
        
        License: Apache 2.0 (permissive, commercial use OK)
        """
        return TrainingConfig(
            base_model="Qwen/Qwen2.5-1.5B-Instruct",
            per_device_train_batch_size=4,
            gradient_accumulation_steps=8,
            lora_r=16,
        )
    
    @staticmethod
    def phi3_mini():
        """Phi-3 Mini 3.8B - High quality, max for 2080 Ti.
        
        License: MIT (most permissive, no restrictions)
        Best for: Commercial products requiring MIT license
        """
        return TrainingConfig(
            base_model="microsoft/Phi-3-mini-4k-instruct",
            per_device_train_batch_size=2,
            gradient_accumulation_steps=16,
            lora_r=16,
            use_qlora=True,  # Required for 3.8B on 11GB
        )
    
    @staticmethod
    def phi3_5_mini():
        """Phi-3.5 Mini 3.8B - Latest Microsoft model.
        
        License: MIT (most permissive, no restrictions)
        Improvements over Phi-3: Better reasoning, multilingual support
        """
        return TrainingConfig(
            base_model="microsoft/Phi-3.5-mini-instruct",
            per_device_train_batch_size=2,
            gradient_accumulation_steps=16,
            lora_r=16,
            use_qlora=True,
        )
    
    @staticmethod
    def heimdall():
        """Heimdall - NornicDB specialized SLM (Apache 2.0 variant).
        
        Optimized for:
        - Cypher query generation
        - NornicDB database operations
        - Heimdall plugin system
        - Graph database management
        
        License: Apache 2.0 (permissive, commercial use OK)
        Base model: Qwen2.5-1.5B-Instruct
        Context size: 2048 tokens (optimal for most Cypher queries)
        Parameter size: 1.5B (efficient, specialized)
        Final size after Q4: ~1.0 GB
        
        Use this when:
        - You prefer Apache 2.0 license (patent protection)
        - You want best efficiency (1.5B parameters)
        - Training on consumer GPU (8-11GB VRAM)
        - You need multilingual support (Qwen excels at this)
        """
        return TrainingConfig(
            base_model="Qwen/Qwen2.5-1.5B-Instruct",
            per_device_train_batch_size=1,  # Minimum for MPS memory limits
            gradient_accumulation_steps=16,  # Increased to maintain effective batch size of 16
            lora_r=16,  # Reduced rank for memory efficiency
            lora_alpha=32,
            learning_rate=3e-4,  # Higher LR for focused domain
            num_train_epochs=10,  # More epochs for specialization
            model_max_length=1024,  # Reduced context for MPS memory limits
            gradient_checkpointing=True,  # Essential for memory efficiency
            dataloader_num_workers=0,  # Reduce memory overhead on MPS
        )
    
    @staticmethod
    def heimdall_mit():
        """Heimdall MIT - NornicDB specialized SLM (MIT license variant).
        
        Optimized for:
        - Cypher query generation
        - NornicDB database operations  
        - Heimdall plugin system
        - Graph database management
        
        License: MIT (most permissive, no restrictions)
        Base model: Phi-3-mini-4k-instruct
        Context size: 2048 tokens (optimal for most Cypher queries)
        Parameter size: 3.8B (higher quality than 1.5B)
        Final size after Q4: ~2.2 GB
        
        Use this when:
        - You require MIT license (strictest commercial requirements)
        - You want highest quality (3.8B parameters)
        - You have 11GB+ VRAM (uses QLoRA)
        - Inference hardware supports 2-3GB models
        
        Training time: ~10-12 hours on 2080 Ti (vs 6-8 for regular Heimdall)
        """
        return TrainingConfig(
            base_model="microsoft/Phi-3-mini-4k-instruct",
            per_device_train_batch_size=4,  # Larger model, smaller batches
            gradient_accumulation_steps=4,
            lora_r=32,  # Higher rank for domain specialization
            lora_alpha=64,
            learning_rate=3e-4,  # Higher LR for focused domain
            num_train_epochs=10,  # More epochs for specialization
            model_max_length=2048,  # Optimized context for Cypher
            use_qlora=True,  # Required for 3.8B on consumer GPU
            gradient_checkpointing=True,
        )


# Default configuration optimized for 2080 Ti
DEFAULT_CONFIG = ModelPreset.qwen_1_5b()


if __name__ == "__main__":
    # Generate example config files
    import os
    os.makedirs("config/model_configs", exist_ok=True)
    
    configs = {
        "tinyllama_1b": ModelPreset.tiny_llama_1b(),
        "qwen_0_5b": ModelPreset.qwen_0_5b(),
        "qwen_1_5b": ModelPreset.qwen_1_5b(),
        "phi3_mini": ModelPreset.phi3_mini(),
        "phi3_5_mini": ModelPreset.phi3_5_mini(),
        "heimdall": ModelPreset.heimdall(),
        "heimdall_mit": ModelPreset.heimdall_mit(),
    }
    
    for name, config in configs.items():
        config.to_yaml(f"config/model_configs/{name}.yaml")
    
    print("✓ Generated preset configurations in config/model_configs/")
