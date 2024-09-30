from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
from huggingface_hub import login

# Replace with your API token
login("")


# Load model and tokenizer
model_name = "meta-llama/Llama-3.1-8B"  # Replace with your preferred model
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(model_name)

# Generate text
input_text = "Create column names for a data table"
input_ids = tokenizer(input_text, return_tensors="pt").input_ids
output = model.generate(input_ids, max_length=50)

# Decode and print the generated text
generated_text = tokenizer.decode(output[0], skip_special_tokens=True)
print(generated_text)
