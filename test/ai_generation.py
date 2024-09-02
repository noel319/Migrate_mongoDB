import requests
import re
from utils.config import general_names
OLLAMA_SERVER_URL = 'http://192.168.10.92:11434/api/generate'

# Pre make name by using regular expression

def regx(column_data):    
    column_name = ''
    flag = False
    patterns = {
        'email': r'[\w\.-]+@[\w\.-]+',
        'phone': r'\+?\d[\d\-\(\) ]{5,}\d',
        'url': r'https?://(?:www\.)?\w+\.\w+',
        'date': r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}',
        'name': r'\b[AА][а-яА-ЯёЁ]+ [AА][а-яА-ЯёЁ]+\b' 
    }
    for name, pattern in patterns.items():
        if re.search(pattern, column_data):
            column_name = name
            flag = True
    return flag, column_name

def generate_name(column_data):
    column_data = ''.json(column_data)
    name_patten = ''.json(general_names)
    flag, column_name = regx(column_data)
    if flag:
        return column_name
    else: 
        prompt = f"""
        You are an AI model designed to help in naming columns for a database. I have data stored in an SQLite database with columns that need appropriate names. The goal is to generate suitable column names based on the data content. You can use the names from the existing list of general names provided, or create new, descriptive names if none of the general names are appropriate.

        ### Task:
        1. Analyze the provided data in each column.
        2. Choose a suitable name from the `general_names` list if it closely matches the data.
        3. If no existing name is a good fit, create a new, descriptive column name.
        4. Ensure the names are clear, concise, and relevant to the data.

        ### Data:
        {column_data}

        ### General Names (pre-existing):
        {name_patten}

        ### Output:
        Do not include descriptions or any additional text, only the column names.
        Return only one column name. 
        """

        payload = {
            'model' : 'llama3.1:70b',
            'prompt' : prompt,
            'stream' : False
        }
        res = requests.post(OLLAMA_SERVER_URL, json=payload)
        if res.status_code == 200:
            str = res.json()['response']
            str = str.split('\n')
            return str[0]
        else:
            return res.raise_for_status()
    
def analyze(chunk):

    return