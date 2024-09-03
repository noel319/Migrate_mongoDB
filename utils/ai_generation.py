import requests
import re
from utils.config import general_names
OLLAMA_SERVER_URL = 'http://192.168.10.92:11434/api/generate'

# Pre make name by using regular expression

phone_pattern = re.compile(r'\+?\d{1,3}[\s\-]?\(?\d{1,5}\)?[\s\-]?\d{1,4}[\s\-]?\d{1,4}[\s\-]?\d{1,4}')
patterns = {
        'email': r'[\w\.-]+@[\w\.-]+',
        'url': r'https?://(?:www\.)?\w+\.\w+',
        'full_name': r'\b[AА][а-яА-ЯёЁ]+ [AА][а-яА-ЯёЁ]+\b' 
    }

def detect_phone(column_data):
    if re.search(phone_pattern, str(column_data)):
        normalized_number = re.sub(r'[^\d]', '', str(column_data))
        if normalized_number.startswith('8') and len(normalized_number) == 11:
            return True,'Phone_number'
        elif normalized_number.startswith('7') and len(normalized_number) == 11:
            return True, 'Phone_number'
        elif len(normalized_number) == 10:
            return True, 'passport'
        elif len(normalized_number) == 6:
            return True, 'passport_number'
        elif len(normalized_number) == 4 and int(normalized_number) > 2030:
            return True, 'passport_series'
    return False, ''
    
def regx(column_data):    
    column_name = ''
    flag = False    
    if column_data.count('') == len(column_data):
        flag = True
        column_name = 'null_column'
        print("welcome to here")
    else:
        for i in range(0,5):
            for name, pattern in patterns.items():
                if re.search(pattern, str(column_data[i])):
                    column_name = name
                    flag = True
                    break
            flag, column_name = detect_phone(str(column_data[i]))
            if flag:
                break
            
    return flag, column_name


def generate_name(df):
    column_names = []
    global general_names
    for col in df.columns:
        column_data = df[col].head().tolist()
        # print(column_data)
        new_name = make_name(column_data)
        count = sum(new_name in item for item in column_names)        
        if new_name not in general_names:            
            general_names.append(new_name)
        if count:
            column_names.append(f"{new_name}_{count}")
        else:
            column_names.append(new_name)
    return column_names
def make_name(column_data):   
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
        5. Do not make name 'age', 'phone', 'email'. If exist 'age' it may be 'id'  
        
        ### Frame Data:
        {column_data}

        ### General Names (pre-existing):
        {general_names}
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
            str = str.split('\n')[0]
            return str
        else:
            return res.raise_for_status()
    
def analyze(chunk):

    return