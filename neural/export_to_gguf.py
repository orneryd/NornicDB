#!/usr/bin/env python3
"""
Export trained model to GGUF format for NornicDB inference.

This script:
1. Merges LoRA adapters (if used) back into base model
2. Converts merged model to FP16 GGUF format
3. Quantizes to smaller format (Q4_K_M, Q5_K_M, Q8_0, etc.)

Example usage:
    # Export with Q4_K_M quantization (recommended)
    python export_to_gguf.py --model_dir models/tlp-evaluator --output tlp-q4.gguf
    
    # Export with different quantization
    python export_to_gguf.py --model_dir models/trained --output model-q8.gguf --quantization Q8_0
    
    # Export FP16 only (no quantization)
    python export_to_gguf.py --model_dir models/trained --output model-fp16.gguf --no_quantize
    
    # Merge LoRA only (for manual conversion)
    python export_to_gguf.py --model_dir models/trained --merge_only --output merged/
"""

import os
import sys
import logging
from pathlib import Path

# Disable SSL verification globally - MUST be done before any HTTP imports
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['HF_HUB_DISABLE_SSL'] = '1'  # HuggingFace specific

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

import argparse
from export import GGUFConverter, merge_lora_adapters, print_usage_instructions

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Export trained model to GGUF format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--model_dir",
        type=str,
        required=True,
        help="Directory containing trained model"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output path for GGUF file (or directory if --merge_only)"
    )
    
    # Conversion options
    parser.add_argument(
        "--quantization",
        type=str,
        default="Q4_K_M",
        choices=[
            "F32", "F16", "Q8_0", "Q6_K",
            "Q5_K_M", "Q5_K_S", "Q4_K_M", "Q4_K_S",
            "Q3_K_M", "Q3_K_S", "Q2_K"
        ],
        help="Quantization type (default: Q4_K_M)"
    )
    parser.add_argument(
        "--no_quantize",
        action="store_true",
        help="Skip quantization, export FP16 only"
    )
    
    # LoRA merging
    parser.add_argument(
        "--base_model",
        type=str,
        help="Base model for LoRA merge (auto-detected from adapter_config.json)"
    )
    parser.add_argument(
        "--merge_only",
        action="store_true",
        help="Only merge LoRA adapters, don't convert to GGUF"
    )
    
    # llama.cpp path
    parser.add_argument(
        "--llama_cpp_path",
        type=str,
        help="Path to llama.cpp repository (auto-detected if not specified)"
    )
    
    return parser.parse_args()


def detect_base_model(model_dir: str) -> str:
    """Detect base model from adapter config."""
    adapter_config = Path(model_dir) / "adapter_config.json"
    
    if not adapter_config.exists():
        return None
    
    try:
        import json
        with open(adapter_config, 'r') as f:
            config = json.load(f)
        base_model = config.get("base_model_name_or_path")
        if base_model:
            logger.info(f"Detected base model: {base_model}")
        return base_model
    except Exception as e:
        logger.warning(f"Failed to read adapter config: {e}")
        return None


def is_lora_model(model_dir: str) -> bool:
    """Check if model directory contains LoRA adapters."""
    adapter_config = Path(model_dir) / "adapter_config.json"
    return adapter_config.exists()


def main():
    """Main export function."""
    # Setup logging
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        level=logging.INFO,
    )
    
    logger.info("⚠️  SSL verification disabled (ignoring certificate errors)")
    
    # Parse arguments
    args = parse_args()
    
    # Verify input
    model_dir = Path(args.model_dir)
    if not model_dir.exists():
        logger.error(f"Model directory not found: {model_dir}")
        sys.exit(1)
    
    # Check if LoRA model
    is_lora = is_lora_model(str(model_dir))
    
    if is_lora:
        logger.info("✓ Detected LoRA adapters")
        
        # Detect or use specified base model
        base_model = args.base_model or detect_base_model(str(model_dir))
        
        if not base_model:
            logger.error("Could not detect base model. Please specify with --base_model")
            sys.exit(1)
        
        # Merge LoRA adapters
        merged_dir = str(model_dir) + "-merged"
        logger.info(f"Merging LoRA adapters to: {merged_dir}")
        
        if not merge_lora_adapters(base_model, str(model_dir), merged_dir):
            logger.error("LoRA merge failed")
            sys.exit(1)
        
        if args.merge_only:
            logger.info(f"✓ Merge complete: {merged_dir}")
            logger.info("Run export again on merged model to convert to GGUF")
            sys.exit(0)
        
        # Use merged model for GGUF conversion
        model_dir = Path(merged_dir)
    else:
        logger.info("Full model detected (no LoRA adapters)")
    
    # Initialize converter
    converter = GGUFConverter(llama_cpp_path=args.llama_cpp_path)
    
    if not converter.check_dependencies():
        logger.error("Missing conversion dependencies")
        sys.exit(1)
    
    # Convert to GGUF
    output_path = args.output
    quantization = None if args.no_quantize else args.quantization
    
    logger.info(f"Converting {model_dir} → {output_path}")
    
    success = converter.convert(
        model_dir=str(model_dir),
        output_path=output_path,
        quantization=quantization,
    )
    
    if not success:
        logger.error("Conversion failed")
        sys.exit(1)
    
    # Print usage instructions
    print_usage_instructions(output_path)
    
    # Cleanup merged directory if created
    if is_lora and not args.merge_only:
        import shutil
        try:
            shutil.rmtree(merged_dir)
            logger.info(f"✓ Cleaned up temporary merge directory")
        except Exception as e:
            logger.warning(f"Failed to cleanup {merged_dir}: {e}")


if __name__ == "__main__":
    main()
