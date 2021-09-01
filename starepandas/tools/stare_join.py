import pystare
import pandas


def stare_join(left_df, right_df, how='left'):
    """ STARE join of two STAREDataFrames.
    Seminal function to geopandas.sjoin().
    At the moment, only the *interesects* predicate is supported.

    Parameters
    ---------------
    left_df: STAREDataFrame
        left dataframe to join
    right_df: STAREDataFrame
        right dataframe to join
    how: str
        either left or innter

    Returns
    ---------
    joined: STAREDataFrame
        Joined data Frame

    Examples
    ----------

    """
    left_key = []
    right_key = []

    for row in right_df.itertuples():
        k = left_df.index[left_df.stare_intersects(row.stare)]
        left_key.extend(list(k))
        right_key.extend([row.Index]*len(k))

    index_map = pandas.DataFrame({'key_left': left_key,
                                  'key_right': right_key})

    if how == 'left':
        joined = left_join(left_df, right_df, index_map)
    elif how == 'inner':
        joined = inner_join(left_df, right_df, index_map)
    
    return joined

        
def inner_join(left_df, right_df, index_map):    
    joined = left_df
    joined = joined.merge(index_map, left_index=True, right_index=True)
    joined = joined.merge(right_df, left_on='_key_right', right_index=True)
    joined = joined.set_index('_key_left')
    joined = joined.drop(["_key_right"], axis=1)
    return joined
        
        
def left_join(left_df, right_df, index_map):
    index_map.set_index('key_left', inplace=True)
    
    joined = left_df
    joined = joined.merge(index_map, left_index=True, right_index=True, how="left")
    joined = joined.merge(right_df,
                          how="left", left_on="key_right",
                          right_index=True, suffixes=("_left", "_right"))
    return joined
