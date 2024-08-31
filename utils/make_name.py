from transformers import pipeline

def load_ai_model():
    """
    Load the pre-trained AI model from Hugging Face.
    """
    return pipeline("text-generation", model="gpt2")
