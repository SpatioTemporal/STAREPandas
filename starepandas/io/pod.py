import starepandas
import glob
import os
import re
import pandas


def read_pods(pod_root, sids=None, tivs=None, pattern=None, add_podname=False, path_format=None):
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
    path_format: str
        default: '{pod_root}/{sid}/{tivs}/'

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

    sids        = None if sids is None else sids
    tivs        = None if tivs is None else tivs
    pattern     = "*" if pattern is None else pattern

    if tivs is None:
        path_format = '{pod_root}/{sid}' if path_format is None else path_format
    else:
        path_format = '{pod_root}/{sid}/{tivs}' if path_format is None else path_format
    
    dfs = []
    for sid in sids:
        for tiv in tivs:
            pod_path = path_format'.format(pod_root=pod_root, sid=sid, tiv=tivs)
            if not os.path.exists(pod_path):
                print('no pod exists for {}'.format(sid))
                continue
            pickles = sorted(glob.glob(os.path.expanduser(pod_path + '/*')))
            search = '.*{pattern}.*'.format(pattern=pattern)
            pods = list(filter(re.compile(search).match, pickles))
            for pod in pods:
                # df = pandas.read_pickle(pod)
                # if add_podname:
                #     df['pod'] = pod
                # dfs.append(df)
                while True:
                    with open(pod,'rb') as input:
                        try:
                            df = pickle.load(input)
                            if add_podname:
                                df['pod'] = pod
                            dfs.append(df)
                        except EOFError as e:
                            break
                    
    df = pandas.concat(dfs)
    df.reset_index(inplace=True, drop=True)
    df = starepandas.STAREDataFrame(df)
    return df

