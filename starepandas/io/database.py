import pandas
import numpy
import starepandas


def read_sql_table(sql, con, stare_column='stare',  **kwargs):
    
    df = pandas.read_sql_table(sql, con)
    df = starepandas.STAREDataFrame(df)
    
    if stare_column in df.columns:        
        df[stare_column] = df[stare_column].apply(func=numpy.frombuffer, args=('int64',))
        df.set_sids(stare_column, inplace=True)
    else:
        print('{} is not in the columns'.format(stare_column))
    return df  


def write_sql_table():
    pass