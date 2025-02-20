import bz2
import glob
import numpy
import os
import pandas
import pickle
import puremagic
import pystare
import re
import starepandas

def generic_open(filename):
    try:
        ext = puremagic.from_file(filename)
    except puremagic.PureError:
        return open
    if ext == '.bz2':
        return bz2.open
    elif ext == '.pickle':
        return open
    else:
        raise Exception("Filetype %s not implemented for %s"%(ext,filename))

def read_pods(pod_root, sids=None, tids=None, pattern=None, add_podname=False, path_format=None, path_delimiter=None,
              temporal_pattern=None, temporal_pattern_tid_index=None, verbose=False):
    """ Reads a STAREDataframe from a directory of STAREPods

    Parameters
    -----------
    path_delimiter: str
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
    temporal_pattern: str
        The regexp including a field containing a tid for matching.
        default: '{pod_path}(.*)-.*'
    temporal_pattern_tid_index: int
        The field of the regexp temporal_pattern from which to extract a tid for matching.
        default: 0
    verbose: bool


    The format of the path to the chunk is as follows.

        pod_path = path_format.format(pod_root=pod_root,delim1=path_delimiter,sid=sid)

        pickles = sorted(glob.glob(os.path.expanduser(pod_path + '/*')))
        search = '.*{pattern}.*'.format(pattern=pattern)
        pods = list(filter(re.compile(search).match, pickles))

    And later on...
                regexp = temporal_pattern.format(pod_path=pod_path)
                p = re.compile(regexp)
                pods_tids = []
                for p_ in pods:
                    m = p.match(p_)
                    if m is not None:
                        tid_ = int(m.groups()[temporal_pattern_tid_index],16)

    A potentially simple way to temporal podding/chunking is as follows...

        <pod-root>/<sid>/<tpod>-<tcover>-<dataset-name-patter>

    E.g.

        /pods/0x0a00000000000004/0x1f4a000000000000_16-0x1f4aa87342001e79-MOD09.A2002299.0710.006.2015151173939.pkl

    With a search pattern of

        temporal_pattern           = '{pod_path}(.*)-(.*)-.*'
        temporal_pattern_tid_index = 1

    As currently written, temporal_pattern_tid_index = 1 will check on temporal overlap with the cover,
    while temporal_pattern_tid_index = 0 will check temporal overlap with the t-pod--but that isn't
    implemented and runs into the issue that the t-pod name isn't currently a valid tid.

     A few examples...

    This is the original case.

    if case == 'spatial':
        tids = None
        pattern='/MOD09.*'

        temporal_pattern           = None
        temporal_pattern_tid_index = None

    The following are emerging functionalities.

    if case == 'granule':
        tids = tids_
        pattern='.*-MOD09.*'

        temporal_pattern           = '{pod_path}(0x.{{16}})-(?!0x).*'
        temporal_pattern_tid_index = 0

    if case == 'tpod':
        tids = tids_
        pattern='.*-MOD09.*'

        tpod_resolution            = 16
        temporal_pattern           = '{pod_path}(0x.{{16}})_%s-(0x.{{16}})-.*'%tpod_resolution
        temporal_pattern_tid_index = 1

    tids should be of the form...

        tids=[ tid1, tid2, ... ]

    Ignore the following for the moment.

        tids=['0x1f98000000000000_16'] -- This is a tpod, (tid-start-zeroed,tpod-resolution).

    We could do a more general cmp_temporal with tids...

        pattern='*-SSMIS.XCAL2016'


    Returns
    --------
    df: starepandas.STAREDataFrame
        A dataframe containing the data of the read pods

    Examples
    ----------
    # >>> import starepandas
    # >>> sdf = starepandas.read_pods(pod_root='tests/data/pods/',
    # ...                             sids=['0x0a00000000000004'],
    # ...                             pattern='SSMIS.XCAL2016',
    # ...                             add_podname=True)
    # >>>
    """

    sids                       = None if sids is None else sids
    tids                       = None if tids is None else tids
    pattern                    = "*" if pattern is None else pattern
    path_delimiter             = '/' if path_delimiter is None else path_delimiter
    temporal_pattern           = '{pod_path}(.*)-.*' if temporal_pattern is None else temporal_pattern
    temporal_pattern_tid_index = 0 if temporal_pattern_tid_index is None else temporal_pattern_tid_index

    if tids is None:
        path_format = '{pod_root}{delim1}{sid}' if path_format is None else path_format
#        tids = [None]
    else:
        # cf. write_pods()
        # path_format = '{pod_root}{delim1}{sid}{delim2}{tid}' if path_format is None else path_format
        path_format = '{pod_root}{delim1}{sid}' if path_format is None else path_format
        tids_cmp = numpy.array(tids).astype(numpy.int64)
        
#-        print('000: ',tids_cmp,list(map(pystare.hex16,tids_cmp)))

#-    print('pod.read_pod')
#-    if tids is not None:
#-        print('tids: ',list(map(pystare.hex16,tids)))
#-    else:
#-        print('tids: ',tids)
    
    dfs = []
    for sid in sids:
        # print('###########################################################################')
        # If we have temporal pods, then the following makes sense.
###        for tid in tids:
        if True:
            if tids is None:
                pod_path = path_format.format(pod_root=pod_root,delim1=path_delimiter,sid=sid)
            else:
                # pod_path = path_format.format(pod_root=pod_root,delim1=path_delimiter,sid=sid,delim2=path_delimiter,tid=tids)
                # Drop temporal chunking for now.
                pod_path = path_format.format(pod_root=pod_root,delim1=path_delimiter,sid=sid)
            if not os.path.exists(pod_path):
                # print('no pod exists for {}'.format(sid))
                # print('no pod exists for {}'.format(pod_path))
                continue
            pickles = sorted(glob.glob(os.path.expanduser(pod_path + '/*')))
            search = '.*{pattern}.*'.format(pattern=pattern)
            pods = list(filter(re.compile(search).match, pickles))
            pods_dict = {}

#+            print('190 pods: ',pods)

            if tids is not None:
                # 1. parse a tid out of a pod name.
                # 2. if the tid is in tids, then keep, else don't load.
                #
                # 1.
#                regexp = pod_path+path_delimiter+'(.*)'+path_delimiter+'(.*)-.*'
#                regexp = pod_path+path_delimiter+'(.*)'
#+                regexp = pod_path+'(.*)-.*'
                regexp = temporal_pattern.format(pod_path=pod_path)
#+                print('regexp: ',regexp)
                p = re.compile(regexp)
                pods_tids = []
                for p_ in pods:
                    m = p.match(p_)
                    if m is not None:
#-                        print('200: ',m.groups())
                        tid_ = int(m.groups()[temporal_pattern_tid_index],16)
#-                        print('201: ',pystare.hex16(tid_),pystare.tiv_utc_to_string(pystare.expanded_tiv(numpy.int64(tid_))))
                        pods_tids.append(tid_)
                        pods_dict[tid_] = p_
#-                print('pods_dict: ',pods_dict)
                #
                # 2.
                pods_to_keep = []
                # 2.1 Make an array of the search tids (done: that's tids_cmp)
                # 2.2 Check each pod tid
                for pt_ in pods_tids:
                    # 2.3 Prepare for the overlap test
                    pt_cmp = numpy.full(tids_cmp.shape,fill_value=pt_,dtype=numpy.int64)
                    idx = None
#-                    print('101: ',pt_cmp,list(map(pystare.hex16,pt_cmp)))
                    cmp = pystare.temporal_value_intersection_if_overlap(tids_cmp,pt_cmp)
#-                    print('102: ',cmp,list(map(pystare.hex16,cmp)))
                    # -1 signals no overlap
                    idx = numpy.where(cmp > 0)[0]
#-                    print('103: ',idx)
                    if len(idx) > 0:
                        pods_to_keep.append(pods_dict[pt_])
#-                        print('104: ',pods_to_keep)
                        continue
#-                    print('105: ',pods_to_keep)
                pods = pods_to_keep

### Note: we could parse out a tid from the path and compare with the tids.
### Somehow only read a file once... If we're doing symlinks... cf. write_pods
###            pods = cull_duplicates(pods)
###
#-            print('300 pods: ',pods)
            for pod in pods:
                not_done = True

                with generic_open(pod)(pod,'rb') as input:
                    if verbose:
                        print('reading ',pod)
                    while not_done:
                        try:
                            #df = pickle.load(input) # Not backwards compatible
                            df = pandas.read_pickle(input)
                            if add_podname:
                                df['pod'] = pod
                            dfs.append(df)
                        except EOFError as e:
                            not_done = False

    if dfs != []:
        df = pandas.concat(dfs)
        df.reset_index(inplace=True, drop=True)
        df = starepandas.STAREDataFrame(df)
    else:
        df = None
    return df



