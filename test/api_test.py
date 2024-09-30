import requests

# Set up your OpenAI API key
api_key = "gbKRLDQp3cVhAw"

# Define the API endpoint
url = "https://os-api.com/api/openai/chat/completions"

# Set the headers for the request
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
}

# Define the body of the request
data = {
    "model": "gpt-4",  # Use the appropriate model name
    "messages": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Tell me about OpenAI."}
    ],
    "max_tokens": 150,  # Limit the number of tokens in the response
    "temperature": 0.7  # Adjusts the creativity of the response
}

# Send the POST request
response = requests.post(url, headers=headers, json=data)

# Check the response status and print the result
if response.status_code == 200:
    print(response.json())
else:
    print(f"Error: {response.status_code}")
    print(response.text)
