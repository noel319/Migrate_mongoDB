from transformers import GPT2LMHeadModel, GPT2Tokenizer
model_name = "gpt2"
model = GPT2LMHeadModel.from_pretrained(model_name)  # Use GPT2LMHeadModel for generation
tokenizer = GPT2Tokenizer.from_pretrained(model_name)
# Make sure to set the model to evaluation mode
model.eval()
import torch

# Define a prompt for generating column names
input_text = "Create column names for a data table"
input_ids = tokenizer(input_text, return_tensors="pt").input_ids

# Generate text
outputs = model.generate(input_ids, max_length=50)

# Decode and print the generated text
generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
print(generated_text)