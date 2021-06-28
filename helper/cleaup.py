
def cleaup_columns(df, char, char2):
    return df.columns.str.replace(char,char2)

def is_empty(df):
    return df.empty


def rename_csv_files(f_str, char, num, file_prefix):
    import datetime
    today = datetime.datetime.today()
    reformat = today.strftime('%Y%m%d')
    rename_str = f_str.split(char)[num] + file_prefix + reformat + ".csv"
    return rename_str.lower()


def stack_columns(filepath, rows_to_skip, keyCols, new_col, new_col_value):
    import pandas as pd
    df = pd.read_csv(filepath, skiprows=rows_to_skip, encoding='utf8')
    df = (df.set_index(keyCols)
          .rename_axis([new_col], axis=1)
          .stack()
          .reset_index())
    df = df.rename(columns={0: new_col_value})
    return df



