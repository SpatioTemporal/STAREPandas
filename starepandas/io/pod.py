import starepandas
import glob
import os
import re
import pandas


def read_pods(pod_root, sids, pattern, add_podname=False):
    """ Reads a STAREDataframe from a directory of STAREPods

    :param add_podname: toggle if the podname should be read as a parameter
    :type add_podname: bool
    :param pod_root: Root directory containing the pods
    :type pod_root: string
    :param sids: STARE index values to read pods for
    :type sids: list/array-like
    :param pattern: name pattern of chunks to read
    :type pattern: str
    :return: starepandas.STAREDataFrame
    :rtype: starepandas.STAREDataFrame
    """

    df = starepandas.STAREDataFrame()
    for sid in sids:
        pod_path = '{pod_root}/{sid}/'.format(pod_root=pod_root, sid=sid)
        if not os.path.exists(pod_path):
            print('no pod exists for {}'.format(sid))
            continue
        pickles = sorted(glob.glob(os.path.expanduser(pod_path + '/*')))
        search = '.*{pattern}.*'.format(pattern=pattern)
        pods = list(filter(re.compile(search).match, pickles))
        for pod in pods:
            df = df.append(pandas.read_pickle(pod))
            if add_podname:
                df['pod'] = pod
    return df

