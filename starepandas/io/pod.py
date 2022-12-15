import starepandas
import glob
import os
import re
import pandas


def read_pods(pod_root
                  , sids=None
                  , tids=None
                  , pattern=None
                  , add_podname=False
                  , path_format=None
                  , path_delimiter=None
                  ):
    """ Reads a STAREDataframe from a directory of STAREPods

    Parameters
    -----------
    pod_root: str
        Root directory containing the pods
    sids: array-like
        STARE index values to read pods for
    tids: array-like
        STARE temporal index values to read pods for
    pattern: str
        name pattern of chunks to read
    add_podname: bool
        toggle if the podname should be read as a parameter
    path_format: str
        default: '{pod_root}/{sid}/'

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

### tids should be of the form...

    tids=[ tid1, tid2, ... ]

### Ignore the following for the moment.

    tids=['0x1f98000000000000_16'] -- This is a tpod, (tid-start-zeroed,tpod-resolution).

### We could do a more general cmp_temporal with tids...

    pattern='*-SSMIS.XCAL2016'

    """

    sids           = None if sids is None else sids
    tids           = None if tids is None else tids
    pattern        = "*" if pattern is None else pattern
    path_delimiter = '/' if path_delimiter is None else path_delimiter
    

    if tids is None:
        path_format = '{pod_root}{delim1}{sid}' if path_format is None else path_format
        tids = [None]
    else:
        # path_format = '{pod_root}{delim1}{sid}{delim2}{tid}' if path_format is None else path_format
        # cf. write_pods
        path_format = '{pod_root}{delim1}{sid}' if path_format is None else path_format
    
    dfs = []
    for sid in sids:
### If we have temporal pods, then the following makes sense.
###        for tid in tids:
        if True:
            if tids[0] is None:
                pod_path = path_format.format(pod_root=pod_root,delim1=path_delimiter,sid=sid)
            else:
                # pod_path = path_format.format(pod_root=pod_root,delim1=path_delimiter,sid=sid,delim2=path_delimiter,tid=tids)
                # Drop temporal chunking for now.
                pod_path = path_format.format(pod_root=pod_root,delim1=path_delimiter,sid=sid)
            if not os.path.exists(pod_path):
                # print('no pod exists for {}'.format(sid))
                print('no pod exists for {}'.format(pod_path))
                continue
            pickles = sorted(glob.glob(os.path.expanduser(pod_path + '/*')))
            search = '.*{pattern}.*'.format(pattern=pattern)
            pods = list(filter(re.compile(search).match, pickles))

            if tid is not None:
                # 1. parse a tid out of a pod name.
                # 2. if the tid is in tids, then keep, else don't load.
                #
                # 1.
                regexp = pod_base+path_delimiter+'(.*)'+path_delimiter+'(.*)-.*'
                p = re.compile(regexp)
                pods_tids = [ p.match(p_).groups()[1] for p_ in pods ]
                #
                # 2.
                pods_to_keep = []
                # 2.1 Make an array of the search tids
                tids_cmp = numpy.array(tids).astype(numpy.int64)
                # 2.2 Check each pod tid
                for pt_ in pods_tids:
                    # 2.3 Prepare for the overlap test
                    pt_cmp = numpy.fill(tids_cmp.shape,fill=pt_,dtype=numpy.int64)
                    idx = None
                    for t_ in tids:
                        cmp = pystare.temporal_value_intersection_if_overlap(tids_cmp,pt_cmp)
                        # -1 signals no overlap
                        idx = numpy.where(cmp > 0)
                    if len(idx) > 0:
                       pods_to_keep.append(pt_)
                       continue
                pods=pods[pods.index(pods_to_keep)]

### Note: we could parse out a tid from the path and compare with the tids.
### Somehow only read a file once... If we're doing symlinks... cf. write_pods
###            pods = cull_duplicates(pods)
###            
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
