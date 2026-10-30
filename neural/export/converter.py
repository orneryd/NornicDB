"""
GGUF export module for trained models.
Converts HuggingFace models to GGUF format for llama.cpp inference.
"""

import os
import sys
import logging
import subprocess
from pathlib import Path
from typing import Optional, List

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

logger = logging.getLogger(__name__)


class GGUFConverter:
    """
    Convert trained models to GGUF format.
    Requires llama.cpp Python bindings or convert.py from llama.cpp repo.
    """
    
    QUANT_TYPES = {
        "F32": "Full 32-bit float (largest, slowest)",
        "F16": "16-bit float (good quality, 50% size)",
        "Q8_0": "8-bit quantization (best quality/size tradeoff)",
        "Q6_K": "6-bit quantization (very good quality)",
        "Q5_K_M": "5-bit quantization (good quality, medium size)",
        "Q5_K_S": "5-bit quantization (good quality, small size)",
        "Q4_K_M": "4-bit quantization (balanced, recommended)",
        "Q4_K_S": "4-bit quantization (smaller, slight quality loss)",
        "Q3_K_M": "3-bit quantization (small, quality loss)",
        "Q3_K_S": "3-bit quantization (very small, more quality loss)",
        "Q2_K": "2-bit quantization (tiny, significant quality loss)",
    }
    
    def __init__(self, llama_cpp_path: Optional[str] = None):
        """
        Initialize converter.
        
        Args:
            llama_cpp_path: Path to llama.cpp repository (optional)
        """
        self.llama_cpp_path = llama_cpp_path or self._find_llama_cpp()
        
        if self.llama_cpp_path:
            logger.info(f"✓ Found llama.cpp at: {self.llama_cpp_path}")
        else:
            logger.warning("⚠️  llama.cpp not found. Will attempt pip install.")
    
    def _find_llama_cpp(self) -> Optional[str]:
        """Try to find llama.cpp installation."""
        # Check common locations
        locations = [
            "lib/llama",
            "../llama.cpp",
            "~/llama.cpp",
            "/usr/local/llama.cpp",
        ]
        
        for loc in locations:
            path = Path(loc).expanduser()
            # Check for new convert_hf_to_gguf.py or old convert.py
            if path.exists() and ((path / "convert_hf_to_gguf.py").exists() or (path / "convert.py").exists()):
                return str(path)
        
        return None
    
    def check_dependencies(self) -> bool:
        """Check if conversion dependencies are available."""
        try:
            import torch
            import transformers
            logger.info("✓ PyTorch and Transformers available")
        except ImportError as e:
            logger.error(f"Missing dependency: {e}")
            return False
        
        # Check for llama.cpp convert script (primary method)
        if self.llama_cpp_path:
            convert_script = Path(self.llama_cpp_path) / "convert_hf_to_gguf.py"
            if not convert_script.exists():
                # Fallback to old convert.py name
                convert_script = Path(self.llama_cpp_path) / "convert.py"
            if convert_script.exists():
                logger.info(f"✓ llama.cpp conversion script available: {convert_script.name}")
                return True
            else:
                logger.warning(f"llama.cpp path found but conversion script missing: {self.llama_cpp_path}")
        
        # Check for llama-cpp-python (secondary, mainly for inference)
        try:
            import llama_cpp
            logger.info("✓ llama-cpp-python available (for inference)")
            logger.warning("⚠️  llama.cpp repository required for conversion")
            logger.warning("   Clone: git clone https://github.com/ggerganov/llama.cpp")
            return False  # Can't convert without llama.cpp repo
        except ImportError:
            logger.info("llama-cpp-python not found")
        
        logger.error("No conversion method available")
        logger.error("")
        logger.error("Required: llama.cpp repository with convert.py")
        logger.error("  git clone https://github.com/ggml-org/llama.cpp")
        logger.error("  cd llama.cpp")
        logger.error("  cmake -B build")
        logger.error("  cmake --build build --config Release")
        logger.error("")
        logger.error("Optional: llama-cpp-python (for inference only)")
        logger.error("  pip install llama-cpp-python")
        return False
    
    def convert_to_fp16(
        self,
        model_dir: str,
        output_path: str,
    ) -> bool:
        """
        Convert model to FP16 GGUF format.
        
        Args:
            model_dir: Directory containing trained model
            output_path: Output GGUF file path
        
        Returns:
            True if successful
        """
        logger.info(f"Converting {model_dir} to FP16 GGUF...")
        
        if not self.llama_cpp_path:
            logger.error("="*60)
            logger.error("llama.cpp repository not found!")
            logger.error("="*60)
            logger.error("")
            logger.error("The conversion requires the llama.cpp repository (not just llama-cpp-python).")
            logger.error("")
            logger.error("Option 1: Clone llama.cpp repository")
            logger.error("  cd ..")
            logger.error("  git clone https://github.com/ggml-org/llama.cpp")
            logger.error("  cd llama.cpp")
            logger.error("  cmake -B build")
            logger.error("  cmake --build build --config Release")
            logger.error("  cd ../neural")
            logger.error("  python export_to_gguf.py --llama_cpp_path ../llama.cpp ...")
            logger.error("")
            logger.error("Option 2: Use llama-cpp-python conversion (if available)")
            logger.error("  pip install llama-cpp-python[convert]")
            logger.error("")
            logger.error("Option 3: Manual conversion")
            logger.error("  Use HuggingFace's convert_to_gguf.py or similar tools")
            logger.error("="*60)
            return False
        
        # Try new script name first, then fallback to old name
        convert_script = Path(self.llama_cpp_path) / "convert_hf_to_gguf.py"
        if not convert_script.exists():
            convert_script = Path(self.llama_cpp_path) / "convert.py"
        
        if not convert_script.exists():
            logger.error(f"Conversion script not found at: {self.llama_cpp_path}")
            logger.error("Expected: convert_hf_to_gguf.py or convert.py")
            logger.error("Make sure llama.cpp is properly cloned")
            return False
        
        try:
            # Run conversion script
            # Both scripts use --outfile and --outtype
            cmd = [
                sys.executable,
                str(convert_script),
                model_dir,
                "--outfile", output_path,
                "--outtype", "f16",
            ]
            
            logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            
            logger.info(result.stdout)
            logger.info(f"✓ Converted to FP16: {output_path}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Conversion failed: {e}")
            logger.error(e.stderr)
            return False
    
    def quantize(
        self,
        input_path: str,
        output_path: str,
        quant_type: str = "Q4_K_M",
    ) -> bool:
        """
        Quantize GGUF model.
        
        Args:
            input_path: Input GGUF file (FP16)
            output_path: Output quantized GGUF file
            quant_type: Quantization type (Q4_K_M, Q5_K_M, Q8_0, etc.)
        
        Returns:
            True if successful
        """
        if quant_type not in self.QUANT_TYPES:
            logger.error(f"Invalid quantization type: {quant_type}")
            logger.error(f"Valid types: {', '.join(self.QUANT_TYPES.keys())}")
            return False
        
        logger.info(f"Quantizing to {quant_type}...")
        logger.info(f"  {self.QUANT_TYPES[quant_type]}")
        
        if not self.llama_cpp_path:
            logger.error("llama.cpp not found")
            return False
        
        # Check for quantize binary in CMake build directory
        quantize_bin = Path(self.llama_cpp_path) / "build" / "bin" / "llama-quantize"
        if not quantize_bin.exists():
            # Fallback to old location (root directory)
            quantize_bin = Path(self.llama_cpp_path) / "quantize"
        
        if not quantize_bin.exists():
            logger.error(f"quantize binary not found: {quantize_bin}")
            logger.error("Build llama.cpp first:")
            logger.error("  cd llama.cpp")
            logger.error("  cmake -B build")
            logger.error("  cmake --build build --config Release")
            return False
        
        try:
            cmd = [
                str(quantize_bin),
                input_path,
                output_path,
                quant_type,
            ]
            
            logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            
            logger.info(result.stdout)
            logger.info(f"✓ Quantized to {quant_type}: {output_path}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Quantization failed: {e}")
            logger.error(e.stderr)
            return False
    
    def convert(
        self,
        model_dir: str,
        output_path: str,
        quantization: Optional[str] = "Q4_K_M",
    ) -> bool:
        """
        Full conversion: Model → FP16 GGUF → Quantized GGUF.
        
        Args:
            model_dir: Directory containing trained model
            output_path: Output GGUF file path
            quantization: Quantization type (None for FP16 only)
        
        Returns:
            True if successful
        """
        # Create temp file for FP16 if quantizing
        if quantization:
            fp16_path = output_path.replace(".gguf", "-fp16.gguf")
        else:
            fp16_path = output_path
        
        # Step 1: Convert to FP16
        if not self.convert_to_fp16(model_dir, fp16_path):
            return False
        
        # Step 2: Quantize (optional)
        if quantization:
            if not self.quantize(fp16_path, output_path, quantization):
                return False
            
            # Remove temp FP16 file
            try:
                os.remove(fp16_path)
                logger.info(f"✓ Removed temporary file: {fp16_path}")
            except Exception as e:
                logger.warning(f"Failed to remove temp file: {e}")
        
        # Verify output
        if not Path(output_path).exists():
            logger.error(f"Output file not created: {output_path}")
            return False
        
        file_size = Path(output_path).stat().st_size / (1024**3)  # GB
        logger.info(f"✓ Output file size: {file_size:.2f} GB")
        
        return True


def merge_lora_adapters(
    base_model: str,
    adapter_dir: str,
    output_dir: str,
) -> bool:
    """
    Merge LoRA adapters back into base model.
    Required before GGUF conversion.
    
    Args:
        base_model: Base model name or path
        adapter_dir: Directory containing LoRA adapters
        output_dir: Output directory for merged model
    
    Returns:
        True if successful
    """
    logger.info("Merging LoRA adapters into base model...")
    logger.info("⚠️  SSL verification disabled (ignoring certificate errors)")
    
    try:
        from transformers import AutoModelForCausalLM, AutoTokenizer
        from peft import PeftModel
        
        # Load base model
        logger.info(f"Loading base model: {base_model}")
        model = AutoModelForCausalLM.from_pretrained(
            base_model,
            trust_remote_code=True,
            torch_dtype="auto",
        )
        
        # Load LoRA adapters
        logger.info(f"Loading LoRA adapters: {adapter_dir}")
        model = PeftModel.from_pretrained(model, adapter_dir)
        
        # Merge
        logger.info("Merging adapters...")
        model = model.merge_and_unload()
        
        # Save merged model
        logger.info(f"Saving merged model: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        model.save_pretrained(output_dir)
        
        # Save tokenizer
        tokenizer = AutoTokenizer.from_pretrained(base_model, trust_remote_code=True)
        tokenizer.save_pretrained(output_dir)
        
        logger.info("✓ Merge complete")
        return True
        
    except Exception as e:
        logger.error(f"Merge failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def print_usage_instructions(output_path: str):
    """Print instructions for using the GGUF model."""
    print("\n" + "="*60)
    print("GGUF Conversion Complete!")
    print("="*60)
    print(f"Output file: {output_path}")
    print()
    print("Next steps:")
    print()
    print("1. Copy to NornicDB models directory:")
    print(f"   cp {output_path} /data/models/")
    print()
    print("2. Use in NornicDB Go code:")
    print("   ```go")
    print("   opts := localllm.DefaultGenerationOptions(")
    print(f'       "/data/models/{Path(output_path).name}"')
    print("   )")
    print("   model, err := localllm.LoadGenerationModel(opts)")
    print("   if err != nil {")
    print("       log.Fatal(err)")
    print("   }")
    print("   defer model.Close()")
    print()
    print("   response, err := model.Generate(ctx, prompt, params)")
    print("   ```")
    print()
    print("3. Or test with llama.cpp CLI:")
    print(f"   ./llama.cpp/main -m {output_path} -p \"Your prompt here\"")
    print("="*60 + "\n")


if __name__ == "__main__":
    # Test conversion dependencies
    logging.basicConfig(level=logging.INFO)
    
    converter = GGUFConverter()
    if converter.check_dependencies():
        print("\n✓ All conversion dependencies available")
        print("\nSupported quantization types:")
        for quant, desc in converter.QUANT_TYPES.items():
            print(f"  {quant:<12} - {desc}")
    else:
        print("\n✗ Missing conversion dependencies")
        print("\nInstall with:")
        print("  pip install llama-cpp-python")
        print("  # OR")
        print("  git clone https://github.com/ggerganov/llama.cpp")
        print("  cd llama.cpp && make")
