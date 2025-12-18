def detect_currency_columns(data):
    data.columns = data.columns.str.strip()
    symbols = ["$", "€", "₱", "¥", "£"]
    columns_with_currency = set() 

    for col in data.columns:
        for value in data[col]:
            value_str = str(value)
            if any(s in value_str for s in symbols):
                columns_with_currency.add(col)
                break


    for col in columns_with_currency:
        data[col] = (
            data[col]
            .astype(str)
            .str.replace(r'[^0-9.]', '', regex=True)
            .astype(float)
        )

    return data  

def find_duplicated_rows(data):
    if data.duplicated().sum() > 0:
        data = data.drop_duplicates()
    return data  

def clean_quantity(df, column_name):
    df[column_name] = (
        df[column_name]
        .astype(str)
        .str.extract(r'(\d+\.?\d*)')   
        .astype(float)                 
    )
    return df

def drop_columns(data, columns_to_drop):
    for col in columns_to_drop:
        if col in data.columns == "":
            data = data.drop(columns=[col])
    return data

    
