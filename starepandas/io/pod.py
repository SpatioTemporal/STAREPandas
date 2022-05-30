import starepandas
import glob
import os
import re
import pandas


def read_pods(pod_root, sids, pattern, add_podname=False):
    """ Reads a STAREDataframe from a directory of STAREPods

    Parameters
    -----------
    pod_root: str
        Root directory containing the pods
    sids: array-like
        STARE index values to read pods for
    pattern: str
        name pattern of chunks to read
    add_podname: bool
        toggle if the podname should be read as a parameter

    Returns
    --------
    df: starepandas.STAREDataFrame
        A dataframe containing the data of the read pods

    Examples:
    ----------
    >>> import starepandas
    >>> sdf = starepandas.read_pods(pod_root='tests/data/pods/',
    ...                             sids=['0x0a00000000000004'],
    ...                             pattern='SSMIS.XCAL2016',
    ...                             add_podname=True)
    >>>
    """
    dfs = []
    for sid in sids:
        pod_path = '{pod_root}/{sid}/'.format(pod_root=pod_root, sid=sid)
        if not os.path.exists(pod_path):
            print('no pod exists for {}'.format(sid))
            continue
        pickles = sorted(glob.glob(os.path.expanduser(pod_path + '/*')))
        search = '.*{pattern}.*'.format(pattern=pattern)
        pods = list(filter(re.compile(search).match, pickles))
        for pod in pods:
            df = pandas.read_pickle(pod)
            if add_podname:
                df['pod'] = pod
            dfs.append(df)
    df = pandas.concat(dfs)
    df.reset_index(inplace=True, drop=True)
    df = starepandas.STAREDataFrame(df)
    return df

