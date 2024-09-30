import requests

# Define the proxy
proxies = {
    'http': 'socks5://0fQyzH:xrMfo0@196.19.123.109:8000',
    'https': 'socks5://0fQyzH:xrMfo0@196.19.123.109:8000'
}

# Example API request using the proxy
url = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-pro:generateContent'

# Define headers and payload
headers = {
    'x-goog-api-key': 'AIzaSyCQOTUyGkea_CY243gzWnFwRVtLWfIYgXM',
    'x-goog-api-client': 'genai-python/0.1.0',  # Indicating the platform as Python
    'accept': 'application/json',
    'accept-charset': 'UTF-8',
    'user-agent': 'Python client',
    'content-type': 'application/json'
}

payload = {
    "model": "models/gemini-1.0-pro",
    "contents": [
        {"role": "user", "parts": [{"text": "Hello World"}]}
    ],
    "generation_config": {
        "temperature": 0.7,
        "top_p": None,
        "top_k": None,
        "candidate_count": None,
        "max_output_tokens": None,
        "stop_sequences": None
    }
}

# Send the request with the proxy
response = requests.post(url, headers=headers, json=payload, proxies=proxies)

# Check the response
if response.status_code == 200:
    json_response = response.json()
    generated_text = json_response['candidates'][0]['content']['parts'][0]['text']
    
    print("Generated Text:", generated_text)
else:
    print("Error:", response.status_code, response.text)
