"""
Main training loop for NornicDB neural models.
Optimized for NVIDIA 2080 Ti with memory-efficient techniques.
"""

import os
import logging
from typing import Optional, Dict
import torch

# Disable SSL verification globally - MUST be done before any HTTP imports
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['HF_HUB_DISABLE_SSL'] = '1'

# Patch requests/urllib3 to disable SSL verification
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

# Patch requests before transformers imports (transformers uses huggingface_hub which uses requests)
# This MUST happen before any imports that use requests
try:
    import requests
    # Store original methods BEFORE patching (critical to avoid recursion)
    _original_session_request = requests.Session.request
    _original_session_send = requests.Session.send
    
    # Disable SSL verification for all requests by patching the Session class
    def patched_request(self, method, url, **kwargs):
        # Force verify=False for all requests
        kwargs['verify'] = False
        # Call the ORIGINAL method, not the patched one
        return _original_session_request(self, method, url, **kwargs)
    requests.Session.request = patched_request
    
    # Also patch the send method
    def patched_send(self, request, **kwargs):
        kwargs['verify'] = False
        # Call the ORIGINAL method, not the patched one
        return _original_session_send(self, request, **kwargs)
    requests.Session.send = patched_send
    
    # Disable warnings
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass
    try:
        requests.packages.urllib3.disable_warnings()
    except (ImportError, AttributeError):
        pass
except Exception as e:
    # If patching fails, continue anyway
    pass

from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    TrainingArguments,
    Trainer,
    BitsAndBytesConfig,
    TrainerCallback,
)
from peft import (
    LoraConfig,
    get_peft_model,
    prepare_model_for_kbit_training,
    TaskType,
)

from training.config import TrainingConfig
from training.dataset import load_dataset
import platform

logger = logging.getLogger(__name__)


def detect_hardware() -> dict:
    """
    Detect available hardware acceleration.
    
    Returns:
        Dictionary with hardware capabilities
    """
    hw = {
        "cuda_available": torch.cuda.is_available(),
        "mps_available": hasattr(torch.backends, "mps") and torch.backends.mps.is_available(),
        "platform": platform.system(),
        "machine": platform.machine(),
        "device": "cpu",
        "device_name": "CPU",
        "memory_gb": 0,
    }
    
    # CUDA (NVIDIA)
    if hw["cuda_available"]:
        hw["device"] = "cuda"
        hw["device_name"] = torch.cuda.get_device_name(0)
        hw["memory_gb"] = torch.cuda.get_device_properties(0).total_memory / 1e9
        hw["compute_capability"] = torch.cuda.get_device_capability(0)
    
    # Metal/MPS (Apple Silicon)
    elif hw["mps_available"]:
        hw["device"] = "mps"
        hw["device_name"] = f"Apple Silicon ({hw['machine']})"
        # MPS doesn't expose memory directly, estimate based on Mac model
        if "M1" in hw["machine"] or "M2" in hw["machine"]:
            hw["memory_gb"] = 16  # Unified memory, conservative estimate
        elif "M3" in hw["machine"]:
            hw["memory_gb"] = 24
        else:
            hw["memory_gb"] = 8
    
    return hw

logger = logging.getLogger(__name__)


class ProgressCallback(TrainerCallback):
    """Custom callback for detailed training progress."""
    
    def __init__(self):
        self.start_time = None
        self.last_log = None
    
    def on_train_begin(self, args, state, control, **kwargs):
        """Called at the beginning of training."""
        import time
        self.start_time = time.time()
        logger.info("=" * 70)
        logger.info("🏋️  TRAINING STARTED")
        logger.info("=" * 70)
    
    def on_log(self, args, state, control, logs=None, **kwargs):
        """Called after logging."""
        if logs:
            import time
            elapsed = time.time() - self.start_time
            hours = int(elapsed // 3600)
            minutes = int((elapsed % 3600) // 60)
            seconds = int(elapsed % 60)
            
            # Calculate progress
            if state.max_steps > 0:
                progress = (state.global_step / state.max_steps) * 100
                
                # Estimate time remaining
                if state.global_step > 0:
                    avg_time_per_step = elapsed / state.global_step
                    remaining_steps = state.max_steps - state.global_step
                    remaining_secs = avg_time_per_step * remaining_steps
                    eta_hours = int(remaining_secs // 3600)
                    eta_minutes = int((remaining_secs % 3600) // 60)
                    
                    logger.info(
                        f"📊 Step {state.global_step}/{state.max_steps} ({progress:.1f}%) | "
                        f"Loss: {logs.get('loss', 0):.4f} | "
                        f"LR: {logs.get('learning_rate', 0):.2e} | "
                        f"Elapsed: {hours:02d}:{minutes:02d}:{seconds:02d} | "
                        f"ETA: {eta_hours:02d}:{eta_minutes:02d}"
                    )
    
    def on_epoch_end(self, args, state, control, **kwargs):
        """Called at the end of an epoch."""
        logger.info("=" * 70)
        logger.info(f"✅ EPOCH {int(state.epoch)} COMPLETE")
        logger.info("=" * 70)
    
    def on_train_end(self, args, state, control, **kwargs):
        """Called at the end of training."""
        import time
        elapsed = time.time() - self.start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        logger.info("=" * 70)
        logger.info(f"🎉 TRAINING COMPLETE!")
        logger.info(f"⏱️  Total time: {hours:02d}:{minutes:02d}:{seconds:02d}")
        logger.info("=" * 70)


class NornicTrainer:
    """
    Trainer for fine-tuning language models.
    Handles LoRA, QLoRA, gradient checkpointing, and memory optimization.
    """
    
    def __init__(self, config: TrainingConfig):
        """
        Initialize trainer with configuration.
        
        Args:
            config: Training configuration
        """
        self.config = config
        self.model = None
        self.tokenizer = None
        self.trainer = None
        
        # Setup logging
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            level=logging.INFO,
        )
        
        # Detect and configure hardware acceleration
        self.device = self._detect_device()
        self._log_hardware_info()
    
    def _detect_device(self) -> str:
        """Detect best available device."""
        hw = detect_hardware()
        
        # Priority: CUDA > MPS > CPU
        if hw["cuda_available"]:
            return "cuda"
        elif hw["mps_available"] and self.config.use_mps:
            # Enable MPS fallback for operations not supported on MPS
            import os
            os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
            return "mps"
        else:
            return "cpu"
    
    def _log_hardware_info(self):
        """Log detected hardware information."""
        hw = detect_hardware()
        
        if hw["device"] == "cuda":
            logger.info(f"🎮 GPU: {hw['device_name']} ({hw['memory_gb']:.1f} GB VRAM)")
        elif hw["device"] == "mps":
            logger.info(f"🍎 Metal: {hw['device_name']}")
        else:
            logger.warning("⚠️  No GPU detected. Training will be slow.")
            logger.info("   Consider using a machine with CUDA GPU or Apple Silicon")
    
    def load_model(self):
        """Load base model with optional quantization."""
        logger.info(f"📦 Loading model: {self.config.base_model}")
        logger.info("⚠️  SSL verification disabled (ignoring certificate errors)")
        
        # Quantization config for QLoRA (4-bit training)
        quantization_config = None
        if self.config.use_qlora:
            logger.info("🔧 Using QLoRA (4-bit quantization)")
            quantization_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_compute_dtype=torch.float16 if self.config.bnb_4bit_compute_dtype == "float16" else torch.bfloat16,
                bnb_4bit_quant_type=self.config.bnb_4bit_quant_type,
                bnb_4bit_use_double_quant=True,  # Nested quantization for extra memory savings
            )
        
        # Load model
        model_kwargs = {
            "trust_remote_code": True,
            "use_cache": False,  # Disable KV cache for training
        }
        
        if quantization_config:
            model_kwargs["quantization_config"] = quantization_config
            model_kwargs["device_map"] = "auto"
        
        # Check for Flash Attention 2 (not available on MPS)
        if self.config.use_flash_attention_2 and self.device != "mps":
            try:
                model_kwargs["attn_implementation"] = "flash_attention_2"
                logger.info("⚡ Flash Attention 2 enabled")
            except Exception as e:
                logger.warning(f"Flash Attention 2 not available: {e}")
        elif self.config.use_flash_attention_2 and self.device == "mps":
            logger.info("ℹ️  Flash Attention 2 not supported on MPS, using default attention")
        
        self.model = AutoModelForCausalLM.from_pretrained(
            self.config.base_model,
            **model_kwargs
        )
        
        # Prepare for k-bit training if using QLoRA
        if self.config.use_qlora:
            self.model = prepare_model_for_kbit_training(self.model)
        
        # Enable gradient checkpointing
        if self.config.gradient_checkpointing:
            self.model.gradient_checkpointing_enable()
            logger.info("✓ Gradient checkpointing enabled")
        
        logger.info("✓ Model loaded")
    
    def load_tokenizer(self):
        """Load tokenizer."""
        logger.info("📝 Loading tokenizer")
        
        # Disable SSL for tokenizer too
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.config.base_model,
            trust_remote_code=True,
        )
        
        # Ensure padding token exists
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
            self.tokenizer.pad_token_id = self.tokenizer.eos_token_id
        
        logger.info("✓ Tokenizer loaded")
    
    def apply_lora(self):
        """Apply LoRA adapters to model."""
        if not self.config.use_lora:
            logger.info("ℹ️  LoRA disabled, training full model")
            return
        
        logger.info(f"🎯 Applying LoRA (rank={self.config.lora_r})")
        
        # LoRA configuration
        peft_config = LoraConfig(
            task_type=TaskType.CAUSAL_LM,
            r=self.config.lora_r,
            lora_alpha=self.config.lora_alpha,
            lora_dropout=self.config.lora_dropout,
            target_modules=self.config.target_modules,
            bias="none",
        )
        
        self.model = get_peft_model(self.model, peft_config)
        
        # Print trainable parameters
        trainable_params = sum(p.numel() for p in self.model.parameters() if p.requires_grad)
        total_params = sum(p.numel() for p in self.model.parameters())
        trainable_pct = 100 * trainable_params / total_params
        
        logger.info(
            f"✓ LoRA applied: {trainable_params:,} / {total_params:,} parameters "
            f"({trainable_pct:.2f}%) trainable"
        )
    
    def prepare_training_args(self) -> TrainingArguments:
        """Prepare HuggingFace TrainingArguments."""
        args = TrainingArguments(
            output_dir=self.config.output_dir,
            
            # Training hyperparameters
            num_train_epochs=self.config.num_train_epochs,
            per_device_train_batch_size=self.config.per_device_train_batch_size,
            per_device_eval_batch_size=self.config.per_device_eval_batch_size,
            gradient_accumulation_steps=self.config.gradient_accumulation_steps,
            learning_rate=self.config.learning_rate,
            weight_decay=self.config.weight_decay,
            max_grad_norm=self.config.max_grad_norm,
            warmup_steps=self.config.warmup_steps,
            
            # Mixed precision
            fp16=self.config.fp16,
            bf16=self.config.bf16,
            
            # Optimization
            optim=self.config.optim,
            gradient_checkpointing=self.config.gradient_checkpointing,
            
            # Logging and checkpointing
            logging_steps=self.config.logging_steps,
            save_steps=self.config.save_steps,
            eval_steps=self.config.eval_steps,
            eval_strategy=self.config.evaluation_strategy,  # Changed from evaluation_strategy in newer transformers
            save_total_limit=self.config.save_total_limit,
            load_best_model_at_end=self.config.load_best_model_at_end,
            metric_for_best_model=self.config.metric_for_best_model,
            
            # Hardware
            dataloader_num_workers=self.config.dataloader_num_workers,
            ddp_find_unused_parameters=self.config.ddp_find_unused_parameters,
            
            # Misc
            seed=self.config.seed,
            report_to=self.config.report_to,
            push_to_hub=False,
        )
        
        return args
    
    def train(self):
        """Run training."""
        logger.info("🚀 Starting training")
        
        # Load model and tokenizer
        if self.model is None:
            self.load_model()
        if self.tokenizer is None:
            self.load_tokenizer()
        
        # Apply LoRA
        self.apply_lora()
        
        # Load dataset
        logger.info(f"📊 Loading dataset: {self.config.dataset_path}")
        datasets = load_dataset(
            path=self.config.dataset_path,
            tokenizer=self.tokenizer,
            max_length=self.config.model_max_length,
            validation_split=self.config.validation_split,
            max_samples=self.config.max_samples,
            seed=self.config.seed,
        )
        
        train_dataset = datasets["train"]
        eval_dataset = datasets["validation"]
        
        # Prepare training arguments
        training_args = self.prepare_training_args()
        
        # Create trainer with progress callback
        self.trainer = Trainer(
            model=self.model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
            tokenizer=self.tokenizer,
            callbacks=[ProgressCallback()],
        )
        
        # Train
        logger.info("🏃 Training started...")
        train_result = self.trainer.train()
        
        # Save final model
        logger.info("💾 Saving model...")
        self.trainer.save_model()
        self.tokenizer.save_pretrained(self.config.output_dir)
        
        # Save training metrics
        metrics = train_result.metrics
        self.trainer.log_metrics("train", metrics)
        self.trainer.save_metrics("train", metrics)
        
        logger.info("✅ Training complete!")
        logger.info(f"📁 Model saved to: {self.config.output_dir}")
        
        return train_result
    
    def evaluate(self):
        """Run evaluation on validation set."""
        if self.trainer is None:
            raise ValueError("Must train or load model before evaluation")
        
        logger.info("📊 Evaluating model...")
        metrics = self.trainer.evaluate()
        
        self.trainer.log_metrics("eval", metrics)
        self.trainer.save_metrics("eval", metrics)
        
        return metrics
    
    def save(self, path: Optional[str] = None):
        """Save model and tokenizer."""
        save_path = path or self.config.output_dir
        
        logger.info(f"💾 Saving to {save_path}")
        self.model.save_pretrained(save_path)
        self.tokenizer.save_pretrained(save_path)
        logger.info("✓ Saved")


def estimate_memory_usage(config: TrainingConfig) -> Dict[str, float]:
    """
    Estimate VRAM usage for training configuration.
    
    Returns:
        Dictionary with memory estimates in GB
    """
    # Rough model size estimates (in billions of parameters)
    model_sizes = {
        "TinyLlama": 1.1,
        "qwen3-0.6b": 0.5,
        "Qwen2.5-1.5B": 1.5,
        "Phi-3": 3.8,
        "Llama-3.2-1B": 1.0,
    }
    
    # Extract model size from name
    model_name = config.base_model
    params = 1.0  # Default
    for key, size in model_sizes.items():
        if key in model_name:
            params = size
            break
    
    # Base model memory (FP16)
    base_memory = params * 2  # 2 bytes per parameter (FP16)
    
    if config.use_qlora:
        base_memory = params * 0.5  # 4-bit quantization
    
    # LoRA parameters (much smaller)
    if config.use_lora:
        lora_params = params * 0.01 * (config.lora_r / 16)  # Roughly 1% of base
        lora_memory = lora_params * 2
    else:
        lora_memory = 0
    
    # Optimizer states (AdamW stores 2x parameters)
    optimizer_memory = (lora_memory if config.use_lora else base_memory) * 2
    
    # Gradients
    gradient_memory = lora_memory if config.use_lora else base_memory
    
    # Activations (depends on batch size and sequence length)
    batch_size = config.per_device_train_batch_size * config.gradient_accumulation_steps
    activation_memory = params * batch_size * config.model_max_length * 4 / 1000  # Rough estimate
    
    if config.gradient_checkpointing:
        activation_memory *= 0.25  # Reduces by ~75%
    
    total = base_memory + lora_memory + optimizer_memory + gradient_memory + activation_memory
    
    return {
        "base_model": base_memory,
        "lora_adapters": lora_memory,
        "optimizer": optimizer_memory,
        "gradients": gradient_memory,
        "activations": activation_memory,
        "total_estimated": total,
    }


if __name__ == "__main__":
    from training.config import ModelPreset
    
    # Test memory estimation
    configs = [
        ("TinyLlama 1B", ModelPreset.tiny_llama_1b()),
        ("Qwen 0.5B", ModelPreset.qwen_0_5b()),
        ("Qwen 1.5B", ModelPreset.qwen_1_5b()),
        ("Phi-3 Mini (QLoRA)", ModelPreset.phi3_mini()),
    ]
    
    print("\n🎮 Memory Estimates for 2080 Ti (11GB VRAM):\n")
    print(f"{'Model':<25} {'Base':<8} {'LoRA':<8} {'Optimizer':<10} {'Gradients':<10} {'Activations':<12} {'Total':<8}")
    print("=" * 95)
    
    for name, config in configs:
        mem = estimate_memory_usage(config)
        print(
            f"{name:<25} "
            f"{mem['base_model']:<8.2f} "
            f"{mem['lora_adapters']:<8.2f} "
            f"{mem['optimizer']:<10.2f} "
            f"{mem['gradients']:<10.2f} "
            f"{mem['activations']:<12.2f} "
            f"{mem['total_estimated']:<8.2f} GB"
        )
