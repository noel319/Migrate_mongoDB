import requests
import re
from utils.config import general_names
from dateutil.parser import parse
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
        for i in range(0,50):
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
    combine_pattern = '|'.join(date_patterns)
    for col in chunk.columns:
        for row in chunk.index:
            # Make Typecial Style IN Phone Number and Passport
            flag, col_name = detect_phone(chunk.loc[row, col])
            if flag and col_name == "Phone_number":
                chunk.loc[row, col] = re.sub(r'[^\d]', '', str(chunk.loc[row, col]))
            elif flag and col_name == "passport":
                chunk.loc[col,row] = f"{str(chunk.loc[row, col])[:4]} No{str(chunk.loc[row, col])[4:]}"
            # Make Typical Style In Date
            matches = re.findall(combine_pattern, str(chunk.loc[row, col]))
            value = str(chunk.loc[row, col])
            try:
                if matches:
                    parse_date = parse(value)
                    if parse_date.year and parse_date.month and parse_date.day:
                        chunk.loc[row, col] = parse_date.strftime('%d.%m.%Y')
                    elif parse_date.year and parse_date.month:
                        chunk.loc[row, col] =  f'01.{parse_date.strftime("%m.%Y")}'
                    elif parse_date.year:
                        chunk.loc[row, col] =  f'01.01.{parse_date.strftime("%Y")}'
            except(ValueError, TypeError, AttributeError):
                print(ValueError)
        column_data = chunk[col].tolist()
        print(f"{column_data} : {detect_outlier(column_data)}")
    


def detect_outlier(column_data):
    prompt = f"""
            I have a dataset with mixed data types including numbers, text, and other values. 
            Your task is to find outliers and return only their indices. 

            ### Dataset:
            {column_data}

            ### What to Identify:
            1. Identify indices of values that stand out significantly from the rest of the data.
            2. Consider numerical outliers, unusual text entries, or any data that seems contextually different.

            ### Output:
            - Do not include any description or text.
            - Provide only the indices of the outliers from the dataset.
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