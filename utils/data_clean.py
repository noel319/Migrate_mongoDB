import pandas as pd

def clean_data(df, ai_model):
    """
    Analyze and clean data: make column names meaningful, handle missing values,
    normalize dates, and remove duplicates.
    """
    df = make_names(df, ai_model)
    df.columns = [col.lower() for col in df.columns]
    df.fillna(method='ffill', inplace=True)
    df.fillna(method='bfill', inplace=True)
    
    for col in df.columns:
        if 'date' in col or 'time' in col:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception:
                pass

    df.drop_duplicates(inplace=True)
    return df

def make_names(df, ai_model):
    """
    Automatically make column names meaningful using AI model.
    """
    for col in df.columns:
        if col.lower() in ['id', 'name', 'email', 'date', 'address', 'phone']:
            continue
        
        if df[col].dtype == 'object':
            sample_data = df[col].dropna().astype(str).sample(min(10, len(df[col]))).tolist()
            new_name = generate_column_name(" ".join(sample_data), ai_model)
        elif df[col].dtype in ['int64', 'float64']:
            new_name = f'{col}_numeric'
        else:
            new_name = col

        df.rename(columns={col: new_name}, inplace=True)
    
    return df

def generate_column_name(sample_data, ai_model):
    """
    Use AI model to generate a meaningful column name.
    """
    prompt = f"Generate a meaningful column name for data that looks like: {sample_data[:100]}..."
    response = ai_model(prompt, max_length=10, num_return_sequences=1)
    generated_name = response[0]['generated_text'].strip()
    return generated_name
