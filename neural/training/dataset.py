"""
Dataset loading and preprocessing for model training.
Supports multiple formats: instruction-following, chat, completion.
"""

import json
from typing import List, Dict, Optional, Union
from enum import Enum
from pathlib import Path
import logging

from datasets import Dataset as HFDataset
from transformers import PreTrainedTokenizer

logger = logging.getLogger(__name__)


class DatasetFormat(Enum):
    """Supported dataset formats."""
    INSTRUCTION = "instruction"  # instruction/input/output
    CHAT = "chat"  # messages array with role/content
    COMPLETION = "completion"  # raw text completion
    AUTO = "auto"  # Auto-detect format


def load_jsonl(path: str, max_samples: Optional[int] = None) -> List[Dict]:
    """Load JSONL file with training examples."""
    data = []
    with open(path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if max_samples and i >= max_samples:
                break
            data.append(json.loads(line))
    return data


def detect_format(samples: List[Dict]) -> DatasetFormat:
    """Auto-detect dataset format from samples."""
    if not samples:
        raise ValueError("Empty dataset")
    
    first = samples[0]
    
    if "messages" in first:
        return DatasetFormat.CHAT
    elif "instruction" in first:
        return DatasetFormat.INSTRUCTION
    elif "text" in first:
        return DatasetFormat.COMPLETION
    else:
        raise ValueError(
            f"Unknown format. Expected 'messages', 'instruction', or 'text'. "
            f"Got keys: {list(first.keys())}"
        )


def format_instruction_example(example: Dict) -> str:
    """
    Format instruction-following example.
    
    Format:
    {
      "instruction": "What to do",
      "input": "The data (optional)",
      "output": "Expected response"
    }
    """
    instruction = example["instruction"]
    input_text = example.get("input", "")
    output = example["output"]
    
    if input_text:
        prompt = f"### Instruction:\n{instruction}\n\n### Input:\n{input_text}\n\n### Response:\n{output}"
    else:
        prompt = f"### Instruction:\n{instruction}\n\n### Response:\n{output}"
    
    return prompt


def format_chat_example(example: Dict, tokenizer: PreTrainedTokenizer) -> str:
    """
    Format chat/conversation example using tokenizer's chat template.
    
    Format:
    {
      "messages": [
        {"role": "system", "content": "..."},
        {"role": "user", "content": "..."},
        {"role": "assistant", "content": "..."}
      ]
    }
    """
    messages = example["messages"]
    
    # Use tokenizer's chat template if available
    if hasattr(tokenizer, "apply_chat_template"):
        try:
            return tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=False
            )
        except Exception as e:
            logger.warning(f"Failed to apply chat template: {e}. Using fallback.")
    
    # Fallback: simple formatting
    formatted = []
    for msg in messages:
        role = msg["role"]
        content = msg["content"]
        if role == "system":
            formatted.append(f"System: {content}")
        elif role == "user":
            formatted.append(f"User: {content}")
        elif role == "assistant":
            formatted.append(f"Assistant: {content}")
    
    return "\n\n".join(formatted)


def format_completion_example(example: Dict) -> str:
    """
    Format completion example (raw text).
    
    Format:
    {
      "text": "The full training text"
    }
    """
    return example["text"]


def preprocess_dataset(
    data: List[Dict],
    tokenizer: PreTrainedTokenizer,
    format: DatasetFormat = DatasetFormat.AUTO,
    max_length: int = 2048,
) -> HFDataset:
    """
    Preprocess dataset for training.
    
    Args:
        data: List of training examples
        tokenizer: Tokenizer for the model
        format: Dataset format (auto-detected if AUTO)
        max_length: Maximum sequence length
    
    Returns:
        HuggingFace Dataset ready for training
    """
    # Auto-detect format
    if format == DatasetFormat.AUTO:
        format = detect_format(data)
        logger.info(f"Auto-detected format: {format.value}")
    
    # Format examples
    formatted_texts = []
    for example in data:
        try:
            if format == DatasetFormat.INSTRUCTION:
                text = format_instruction_example(example)
            elif format == DatasetFormat.CHAT:
                text = format_chat_example(example, tokenizer)
            elif format == DatasetFormat.COMPLETION:
                text = format_completion_example(example)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            formatted_texts.append(text)
        except Exception as e:
            logger.warning(f"Failed to format example: {e}. Skipping.")
            continue
    
    logger.info(f"Formatted {len(formatted_texts)}/{len(data)} examples")
    
    # Tokenize
    def tokenize_function(examples):
        # Tokenize with truncation and padding
        result = tokenizer(
            examples["text"],
            truncation=True,
            max_length=max_length,
            padding="max_length",
            return_tensors=None,
        )
        
        # For causal LM, labels are the same as input_ids
        result["labels"] = result["input_ids"].copy()
        
        return result
    
    # Create HuggingFace dataset
    dataset = HFDataset.from_dict({"text": formatted_texts})
    
    # Tokenize in batches
    tokenized = dataset.map(
        tokenize_function,
        batched=True,
        remove_columns=dataset.column_names,
        desc="Tokenizing dataset",
    )
    
    return tokenized


def load_dataset(
    path: str,
    tokenizer: PreTrainedTokenizer,
    format: DatasetFormat = DatasetFormat.AUTO,
    max_length: int = 2048,
    validation_split: float = 0.1,
    max_samples: Optional[int] = None,
    seed: int = 42,
) -> Dict[str, HFDataset]:
    """
    Load and preprocess dataset from JSONL file.
    
    Args:
        path: Path to JSONL file
        tokenizer: Tokenizer for the model
        format: Dataset format (auto-detected if AUTO)
        max_length: Maximum sequence length
        validation_split: Fraction of data for validation
        max_samples: Limit number of samples (for testing)
        seed: Random seed for train/val split
    
    Returns:
        Dictionary with 'train' and 'validation' datasets
    """
    logger.info(f"Loading dataset from {path}")
    
    # Load JSONL
    data = load_jsonl(path, max_samples=max_samples)
    logger.info(f"Loaded {len(data)} examples")
    
    # Preprocess
    dataset = preprocess_dataset(data, tokenizer, format, max_length)
    
    # Split train/validation
    if validation_split > 0:
        split = dataset.train_test_split(
            test_size=validation_split,
            seed=seed,
        )
        train_dataset = split["train"]
        eval_dataset = split["test"]
        logger.info(
            f"Split: {len(train_dataset)} train, {len(eval_dataset)} validation"
        )
    else:
        train_dataset = dataset
        eval_dataset = None
        logger.info(f"No validation split: {len(train_dataset)} train examples")
    
    return {
        "train": train_dataset,
        "validation": eval_dataset,
    }


def create_example_datasets():
    """Create example datasets for testing."""
    import os
    
    os.makedirs("data/examples", exist_ok=True)
    
    # TLP Evaluation Dataset
    tlp_data = [
        {
            "instruction": "Evaluate the TLP (Traffic Light Protocol) level for this data",
            "input": "Customer email addresses and phone numbers",
            "output": "TLP:AMBER - Limited distribution. Contains PII that should only be shared with specific organizations."
        },
        {
            "instruction": "Evaluate the TLP (Traffic Light Protocol) level for this data",
            "input": "Public API documentation",
            "output": "TLP:CLEAR - Public information that can be freely shared."
        },
        {
            "instruction": "Evaluate the TLP (Traffic Light Protocol) level for this data",
            "input": "Internal salary data and compensation details",
            "output": "TLP:RED - Highly confidential. Must not be shared outside the organization."
        },
        {
            "instruction": "Evaluate the TLP (Traffic Light Protocol) level for this data",
            "input": "Security vulnerability report for internal systems",
            "output": "TLP:AMBER+STRICT - Confidential information that can only be shared with named recipients."
        },
        {
            "instruction": "Evaluate the TLP (Traffic Light Protocol) level for this data",
            "input": "Company blog post about new product features",
            "output": "TLP:CLEAR - Public marketing content intended for broad distribution."
        },
    ]
    
    with open("data/examples/tlp_evaluation.jsonl", 'w') as f:
        for item in tlp_data:
            f.write(json.dumps(item) + '\n')
    
    # Chat Format Example
    chat_data = [
        {
            "messages": [
                {"role": "system", "content": "You are a helpful TLP classification assistant."},
                {"role": "user", "content": "Classify this data: Internal meeting notes"},
                {"role": "assistant", "content": "TLP:AMBER - Internal use only, limited distribution."}
            ]
        },
        {
            "messages": [
                {"role": "system", "content": "You are a helpful TLP classification assistant."},
                {"role": "user", "content": "Classify this data: Public press release"},
                {"role": "assistant", "content": "TLP:CLEAR - Public information, unrestricted sharing."}
            ]
        },
    ]
    
    with open("data/examples/tlp_chat.jsonl", 'w') as f:
        for item in chat_data:
            f.write(json.dumps(item) + '\n')
    
    logger.info("✓ Created example datasets in data/examples/")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_example_datasets()
