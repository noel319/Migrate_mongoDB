import requests
import re, aiohttp
from utils.config import general_names
import warnings
import pandas as pd

url = "https://os-api.com/api/openai/chat/completions"
api_key = ""
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
}

# Pre make name by using regular expression
INT32_MIN = -(2**31)
INT32_MAX = 2**31 - 1
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
def is_float(value):
    # Check if the value can be converted to a float
    try:
        # Check if the value is an float within the int32 range
        val = float(value)
        if INT32_MIN <= val <= INT32_MAX:
            return True
        else:
            return False
    except (ValueError, TypeError):
        return False

def is_date(date_str):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            pd.to_datetime(date_str, dayfirst=True)
        return True
    except (ValueError, TypeError):
        return False

def is_valid_int(value):
    try:
        # Check if the value is an integer within the int32 range
        val = int(value)
        if INT32_MIN <= val <= INT32_MAX:
            return True
        else:
            return False
    except (ValueError, TypeError):
        return False

def detect_type(df, col):
    # Initialize counters for different data types
    dtype_counts = {
        'string': 0,
        'int_string': 0,
        'float_string': 0,
        'date': 0
    }
    # Analyze each value in the column
    for value in df[col]:
        if isinstance(value, str):
            if is_valid_int(value):  # Check for valid integer strings
                dtype_counts['int_string'] += 1
            elif is_float(value):  # Check for float strings
                dtype_counts['float_string'] += 1
            elif is_date(value) and value != '':  # Check for valid date strings
                dtype_counts['date'] += 1
            elif value != '':  # Otherwise, it's a regular string
                dtype_counts['string'] += 1
        elif isinstance(value, (int, float)):  # Handle numeric types
            if isinstance(value, int) and INT32_MIN <= value <= INT32_MAX:
                dtype_counts['int_string'] += 1
            elif isinstance(value, float) and INT32_MIN <= value <= INT32_MAX:
                dtype_counts['float_string'] += 1
            else:
                dtype_counts['string'] += 1  # Handle large numbers outside int32 range
        elif isinstance(value, pd.Timestamp) and pd.notnull(value):  # Handle Timestamp objects
            dtype_counts['date'] += 1
        elif pd.notnull(value):
            dtype_counts['string'] += 1
    column_type = (max(dtype_counts, key=dtype_counts.get))
    return column_type

def detect_phone(column_data):
    if re.search(phone_pattern, str(column_data)):        
        normalized_number = re.sub(r'[^a-zA-Zа-яА-Я0-9\s]', '', str(column_data))
        num_len = len(re.sub(r'[^\d]', '', str(column_data)))
        if normalized_number.startswith('8') and len(normalized_number) == 11:
            return True,'phone_number'
        elif normalized_number.startswith('7') and len(normalized_number) == 11:
            return True, 'phone_number'
        elif len(normalized_number) == 10 and num_len ==10:
            return True, 'passport'
        elif len(normalized_number) == 6 and num_len == 6:
            return True, 'passport_number'
        elif len(normalized_number) == 4 and num_len == 4:
            return True, 'passport_series'
    return False, ''



def regx(column_data):    
    column_name = ''
    flag = False
    name_type = {'phone_number':0, 'passport':0, 'passport_series':0, 'passport_number':0, 'null':0}
    if column_data.count('') == len(column_data):
        flag = True
        column_name = 'null_column'
        
    else:
        for i in range(0,len(column_data)):
            for name, pattern in patterns.items():
                if re.search(pattern, str(column_data[i])):
                    column_name = name
                    flag = True
                    return flag, column_name
            flag, column_name = detect_phone(str(column_data[i]))
            if flag:
                name_type[column_name] += 1 
            else:
                name_type['null'] += 1
        most_name = max(name_type, key=name_type.get)
        if most_name == 'null':
            return False, ''
        else:
            return True, most_name
    return flag, column_name


async def generate_name(df):
    
    column_names = []
    column_tpye = []
    global general_names
    for col in df.columns:
        column_tpye.append(detect_type(df, col))
        column_data = df[col].tolist()
        new_name = await make_name(column_data)
        count = sum(new_name in item for item in column_names)        
        if new_name not in general_names:            
            general_names.append(new_name)
        if count:
            column_names.append(f"{new_name}_{count}")
        else:
            column_names.append(new_name)
    return column_names, column_tpye
async def make_name(column_data):   
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
        data = {
                "model": "gpt-4",  # Use the model you need
                "messages": [
                    {"role": "system", "content": "You are a creative assistant."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 200,  # Adjust based on the desired response length
                "temperature": 0.8  # Increase for more creative responses
            }
        
        # Using aiohttp for asynchronous HTTP requests
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:                    
                    result = await response.json()
                    if len(result['choices'][0]['message']['content']) < 50:
                        column_name = result['choices'][0]['message']['content']
                    else:
                        if re.findall(r"`(.*?)`", result['choices'][0]['message']['content']):
                            column_name = re.findall(r"`(.*?)`", result['choices'][0]['message']['content'])[0]
                        elif re.findall(r"'(.*?)'", result['response']):
                            column_name = re.findall(r"'(.*?)'", result['choices'][0]['message']['content'])[0]
                        elif re.findall(r'"(.*?)"', result['response']):
                            column_name = re.findall(r'"(.*?)"', result['choices'][0]['message']['content'])[0]
                    return column_name
                else:
                    return "unknow_name"
                    # Raise an exception if the status code indicates an error
                    
        
def analyze(chunk, column_type):
    
    chunk.drop_duplicates(inplace = True)
    for col in chunk.columns:        
        # Make Typecial Style IN Phone Number and Passport
        if col == "Phone_number":
            try:
                chunk[col] = chunk[col].str.replace(r'[^\d,]', '', regex = True)
            except ValueError:
                pass
        elif col == "passport":
            try:
                chunk[col] = chunk[col].astype(str).str[:4] + 'No' + chunk[col].astype(str).str[4:]
            except ValueError:
                pass
        if column_type[chunk.columns.get_loc(col)] == 'string':
            chunk[col] = chunk[col].astype(str)
        elif column_type[chunk.columns.get_loc(col)]  == 'int_string':
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0).astype(int)
        elif column_type[chunk.columns.get_loc(col)]  == 'float_string':
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype(float)
        elif column_type[chunk.columns.get_loc(col)]  == 'date':        
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
        # Convert to datetime and replace invalid dates with empty string
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce', dayfirst=True)
            chunk[col] = chunk[col].astype(object).where(chunk[col].notnull(), None)
    return chunk
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
