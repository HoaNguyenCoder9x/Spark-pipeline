def columns_formatted(df):
    converted_cols = {}
    for col in df.columns:
        new_col = col.replace(' ','_')\
            .lower()\
            .strip()
        converted_cols[col] = new_col
    return converted_cols