import requests
import re, ast
from utils.config import general_names
from dateutil.parser import parse
import pandas as pd
OLLAMA_SERVER_URL = 'http://192.168.10.92:11434/api/generate'

# Pre make name by using regular expression

phone_pattern = re.compile(r'\+?\d{1,3}[\s\-]?\(?\d{1,5}\)?[\s\-]?\d{1,4}[\s\-]?\d{1,4}[\s\-]?\d{1,4}')
patterns = {
        'email': r'[\w\.-]+@[\w\.-]+',
        'url': r'https?://(?:www\.)?\w+\.\w+',
        'full_name': r'\b[AА][а-яА-ЯёЁ]+ [AА][а-яА-ЯёЁ]+\b' 
    }
date_patterns = [
        r'\b\w{3} \d{1,2}, \d{4}\b', #Matches sep 20, 2002
        r'\b\d{4}-\d{1,2}-\d{1,2}\b',  # Matches YYYY-MM-DD
        r'\b\d{1,2}\.\d{1,2}\.\d{4}\b',  # Matches DD.MM.YYYY
        r'\b\d{1,2}/\d{1,2}/\d{4}\b', #Matches DD/MM/YYYY
    ]

def convert_to_date(date_str):
    try:
        return pd.to_datetime(date_str, errors='raise')
    except:
        try:
            return pd.to_datetime(date_str, format='%Y', errors='raise')
        except:
            return date_str

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
        for i in range(0,10):
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
        column_data = df[col].tolist()
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
    num_list = []
    chunk.drop_duplicates(inplace = True)
    for col in chunk.columns:        
        # Make Typecial Style IN Phone Number and Passport
        if col == "Phone_number":
            chunk[col] = chunk[col].str.replace(r'[^\d]', '', regex = True)
        elif col == "passport":
            chunk[col] = chunk[col].astype(str).str[:4] + 'No' + chunk[col].astype(str).str[4:]
        # Make Typical Style In Date
        chunk[col] = chunk[col].apply(convert_to_date)
        column_data = chunk[col].tolist()
        try:
            result = detect_outlier(column_data)
            num_list = num_list + ast.literal_eval(result)
        except (ValueError, SyntaxError) as e:
            print("Error evaluatin the result:", e)
    num_list = list(set(num_list))        
    print(f"Main table error rows: {num_list}")
    df = chunk.iloc[num_list]
    print(df)
    # new_df = rearange(df)

def rearange(df):
    prompt = f"""
    """
    payload = {
        'model' : 'llama3.1:70b',
        'prompt' : prompt,
        'stream' : False
    }
    res = requests.post(OLLAMA_SERVER_URL, json=payload)
    if res.status_code == 200:
        str = res.json()['response']
        # str = str.split('\n')[0]
        return str
    else:
        return res.raise_for_status()

def detect_outlier(column_data):
    prompt = f"""
            You will be provided with dataset delimited by triple backticks.
            <task>
            1. Analyze the dataset and understand the basic data format and meaning.
            2. Select values ​​that differ significantly from the general data format and meaning.
                #For example
                -If the numeric value dataset contains the text, select the text.
                -If the text dataset contains numeric values, select the numeric values.
                -If the date dataset contains the text or numeric values, select the text or numeric values.
                -If there is a value that is particularly larger or smaller than the basic numeric dataset in the numeric value data, select that value.
                -If the text data contains dataset that is the same as the hash value, select the hash value.
                -Ignore empty or null characters and do not include them in the selection.
            3. Count the positions of the selected values ​​in the dataset. The start value is 0.
            4. Make a list of these count number.
                -If an error occurs or it is difficult to select a value, return an empty list.
                -Do not include descriptions or any additional text, only the make count list.
            </task>

            <Dataset>
            ```{column_data}```
            </Dataset>

            <output>            
            Only output in this format:[1, 20,  21, 30]            
            </output>
        """

    payload = {
            'model' : 'llama3.1:70b',
            'prompt' : prompt,
            'stream' : False
        }
    res = requests.post(OLLAMA_SERVER_URL, json=payload)
    if res.status_code == 200:
        str = res.json()['response']
        # str = str.split('\n')[0]
        return str
    else:
        return res.raise_for_status()