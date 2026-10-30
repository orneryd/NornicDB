# Model License Reference

All base models used for fine-tuning with **permissive licenses** that allow commercial use.

---

## Licensed Models

### MIT License (Most Permissive)

**Microsoft Phi-3 Models** - No restrictions, full commercial use

| Model | Size | Context | License | Use Case |
|-------|------|---------|---------|----------|
| [Phi-3-mini-4k-instruct](https://huggingface.co/microsoft/Phi-3-mini-4k-instruct) | 3.8B | 4K | MIT | High quality, max for 2080 Ti |
| [Phi-3.5-mini-instruct](https://huggingface.co/microsoft/Phi-3.5-mini-instruct) | 3.8B | 128K | MIT | Latest, better reasoning |

**License URL:** https://huggingface.co/microsoft/Phi-3-mini-4k-instruct/blob/main/LICENSE

**Commercial use:** ✅ Yes, no restrictions  
**Redistribution:** ✅ Yes, with attribution  
**Modifications:** ✅ Yes, freely  

---

### Apache 2.0 License (Permissive)

**Qwen2.5 Models** - Alibaba's efficient models, permissive license

| Model | Size | Context | License | Use Case |
|-------|------|---------|---------|----------|
| [qwen3-0.6b-Instruct](https://huggingface.co/Qwen/qwen3-0.6b-Instruct) | 0.5B | 32K | Apache 2.0 | Ultra-efficient, specific tasks |
| [Qwen2.5-1.5B-Instruct](https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct) | 1.5B | 32K | Apache 2.0 | **Heimdall default**, balanced |
| [Qwen2.5-3B-Instruct](https://huggingface.co/Qwen/Qwen2.5-3B-Instruct) | 3B | 32K | Apache 2.0 | Better quality, needs QLoRA |
| [Qwen2.5-7B-Instruct](https://huggingface.co/Qwen/Qwen2.5-7B-Instruct) | 7B | 128K | Apache 2.0 | Best quality, requires >16GB VRAM |

**License URL:** https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct/blob/main/LICENSE

**Commercial use:** ✅ Yes, no restrictions  
**Redistribution:** ✅ Yes, with attribution  
**Patent grant:** ✅ Yes, protection included  

---

**TinyLlama Models** - Fast, lightweight models

| Model | Size | Context | License | Use Case |
|-------|------|---------|---------|----------|
| [TinyLlama-1.1B-Chat-v1.0](https://huggingface.co/TinyLlama/TinyLlama-1.1B-Chat-v1.0) | 1.1B | 2K | Apache 2.0 | Fastest training, experiments |

**License URL:** https://github.com/jzhang38/TinyLlama/blob/main/LICENSE

**Commercial use:** ✅ Yes, no restrictions  
**Redistribution:** ✅ Yes, with attribution  

---

## ❌ Restrictive Licenses (Not Recommended)

### Llama Models (Meta)

**Llama 3.2 Community License** - Has usage restrictions

- ✅ Free for most uses
- ⚠️ Restrictions if you have >700M monthly active users
- ⚠️ Cannot use to train other LLMs without permission
- ⚠️ Additional terms beyond standard open source

**Not included by default** - Use Phi-3 (MIT) or Qwen (Apache 2.0) instead.

---

## License Comparison

| License | Commercial | Attribution | Patents | Restrictions |
|---------|-----------|-------------|---------|--------------|
| **MIT** | ✅ Full | Required | No grant | None |
| **Apache 2.0** | ✅ Full | Required | ✅ Granted | None |
| **Llama 3.2** | ⚠️ Limited | Required | Some | Usage caps |

---

## Heimdall Variants Comparison

### Choosing Between Apache 2.0 and MIT

We provide **two Heimdall presets** to meet different licensing needs:

| Feature | Heimdall (Apache 2.0) | Heimdall MIT |
|---------|----------------------|---------------|
| **License** | Apache 2.0 | MIT |
| **Base Model** | Qwen2.5-1.5B-Instruct | Phi-3-mini-4k-instruct |
| **Parameters** | 1.5B | 3.8B |
| **Training Time** | 6-8 hours | 10-12 hours |
| **VRAM Required** | 8-9 GB | 10-11 GB |
| **Final Size (Q4)** | ~1.0 GB | ~2.2 GB |
| **Inference VRAM** | ~1.5 GB | ~2.5 GB |
| **Quality** | Excellent | Superior |
| **Multilingual** | Excellent | Good |
| **Training Command** | `--preset heimdall` | `--preset heimdall_mit` |

### When to Use Apache 2.0 Variant ✅ **Recommended**

**Best for most users:**
- ✅ Apache 2.0 is permissive enough for 99% of commercial use cases
- ✅ Includes explicit patent grant (protects you from patent trolls)
- ✅ Faster training and smaller model
- ✅ Better multilingual support (Qwen excels at non-English)
- ✅ More efficient for edge deployment

**Use cases:**
- SaaS products
- Commercial applications
- Embedded systems
- Mobile deployment
- Cloud services

### When to Use MIT Variant

**Best for strict legal requirements:**
- ✅ Some enterprises require MIT license specifically
- ✅ Simplest possible license (2 paragraphs)
- ✅ Higher quality output (3.8B vs 1.5B parameters)
- ✅ Better reasoning on complex queries

**Use cases:**
- Enterprise deployments with strict legal requirements
- Government contracts requiring MIT
- Academic research (MIT often required)
- Maximum quality regardless of size

### License Comparison: Apache 2.0 vs MIT

| Aspect | Apache 2.0 ✅ **Recommended** | MIT |
|--------|-------------------------------|-----|
| **Commercial use** | ✅ Yes | ✅ Yes |
| **Redistribution** | ✅ Yes | ✅ Yes |
| **Modification** | ✅ Yes | ✅ Yes |
| **Patent protection** | ✅ **Explicit grant** | ⚠️ Not mentioned |
| **Trademark** | Protected | Not mentioned |
| **License length** | ~5 pages | ~2 paragraphs |
| **Simplicity** | Moderate | ✅ **Simplest** |
| **Legal certainty** | ✅ **Very high** | High |

**Bottom line:** Apache 2.0 provides more legal protection (patents), MIT is simpler. Both are fully permissive for commercial use.

---

## Recommendations by Use Case

### For Commercial Products
**Best:** Phi-3 (MIT) or Qwen2.5 (Apache 2.0)
- No restrictions on usage
- Full redistribution rights
- Patent protection (Apache 2.0)

### For NornicDB Heimdall

**Default (Recommended):** `--preset heimdall` (Apache 2.0)
- Base: Qwen2.5-1.5B-Instruct
- Best balance of quality/efficiency/size
- Permissive license with patent protection
- 32K context window
- Excellent multilingual support
- Final size: ~1.0 GB

**MIT Variant:** `--preset heimdall_mit` (MIT)
- Base: Phi-3-mini-4k-instruct  
- Highest quality output
- Simplest license
- Better reasoning ability
- Final size: ~2.2 GB

### For Maximum Performance
**Option 1:** Phi-3.5-mini (MIT, 3.8B) - Requires QLoRA  
**Option 2:** Qwen2.5-3B (Apache 2.0) - Better than Phi-3  

---

## Verifying Licenses

Before using any model:

1. **Check HuggingFace page** - Look for LICENSE file
2. **Read terms** - Understand restrictions
3. **Commercial use** - Verify explicitly allowed
4. **Attribution** - Note requirements

**Quick check:**
```bash
# Download license file
wget https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct/resolve/main/LICENSE

# Read it
cat LICENSE
```

---

## Legal Disclaimer

This document provides general information only. Always verify licenses directly from model repositories. License terms may change. Consult legal counsel for commercial deployments.

**Last updated:** December 6, 2025
