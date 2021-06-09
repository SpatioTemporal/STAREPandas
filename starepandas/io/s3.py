import h5py
import netCDF4
import pyhdf.SD
import re
from io import BytesIO
import boto3
# Why?
import os
import tempfile


def parse_s3_url(url):
    """Parse an s3-ish URL. Assume the last bit after a / is a file. Force user to add / to end.
    Return a dictionary with the tokens {bucket_name, prefix, prefix_end(/), and the resource."""
    ps3u = re.compile('^s3://([^/]*)/(.*)(/)(.*)$')
    ps3u = re.compile('^s3://([^/]*)(.*)(/)(.*)$')
    tokens = {}
    m = ps3u.match(url)
    token_names = ['bucket_name', 'prefix',  'prefix_end', 'resource']
    if m:
        try:
            t_ = m.groups()
        except:
            t_[0] = m.group()
    for i in range(len(t_)):
        tokens[token_names[i]] = t_[i]
    try:
        tokens['prefix'] = t_[1][1:]
    except:
        pass
    if tokens['prefix'] == '' and tokens['resource'] == '':
        tokens['prefix_end'] = ''

    return tokens


def get_s3_keys(bucket_name, s3_client, prefix=''):
    """
    Generate the keys in an S3 bucket.

    :param s3_client:
    :type s3_client:
    :param bucket_name:
    :type bucket_name:

    :param prefix: Only fetch keys that start with this prefix (optional).
    """

    kwargs = {'Bucket': bucket_name}

    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    done = False
    while not done:
        resp = s3_client.list_objects_v2(**kwargs)
        try:
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix):
                    # print('key: ',key)
                    yield key
        except KeyError:
            print('Empty response from s3 for bucket %s with prefix %s' % (bucket_name, prefix))
            break

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def s3_glob(path, pattern=None, s3_client=None):
    if s3_client is None:
        s3_client = boto3.client('s3')
    s3_tokens = parse_s3_url(path)
    keys = get_s3_keys(
        s3_tokens['bucket_name']
        , s3_client
        , prefix=s3_tokens['prefix'] + s3_tokens['prefix_end'] + s3_tokens['resource']
    )
    names = []
    if pattern is not None:
        r = re.compile(pattern)
    # TODO Replace by wrapping stream.
    for k in keys:
        if pattern is not None:
            if r.match(k):
                names.append(k)
        else:
            names.append(k)
    return names, (s3_tokens, s3_client,)


def h5_dataset_from_s3(s3_client, bucket_name, key, filename='file.h5'):
    buff = BytesIO()
    s3_client.download_fileobj(bucket_name, key, buff)
    buff.name = filename
    buff.seek(0)
    return h5py.File(buff, 'r')  # returns an hdf5 file object


def hdf4_dataset_from_s3(s3_client, bucket_name, key, filename='file.hdf'):
    tmpdir = tempfile.TemporaryDirectory()
    full_filename = os.path.join(tmpdir.name, filename)
    s3_client.download_file(bucket_name, key, full_filename)
    return pyhdf.SD.SD(full_filename, pyhdf.SDC.READ), tmpdir, full_filename  # returns an SDS


# Remember to cleanup! tmpdir.cleanup()


def with_hdf4_get(hdf, var, attrs=None):
    sds = hdf.select(var)
    ret = sds.get()
    if attrs is not None:
        d = {}
        for i in attrs:
            d[i] = sds.attributes()[i]
    sds.endaccess()
    if attrs is not None:
        return ret, d
    return ret


def nc4_dataset_from_s3(s3_client, bucket_name, key, filename='file.nc4'):
    buff = BytesIO()
    s3_client.download_fileobj(bucket_name, key, buff)
    buff.name = filename
    buff.seek(0)
    return netCDF4.Dataset(filename, memory=buff.read(), diskless=True, mode='r')


def nc4_dataset_wrapper(file_path, mode='r', format=None):
    ds = None
    if 's3://' == file_path[0:5]:
        s3 = parse_s3_url(file_path)
        s3_client = boto3.client('s3')
        ds = nc4_dataset_from_s3(s3_client, s3['bucket_name'], s3['prefix'] + s3['prefix_end'] + s3['resource'],
                                 filename=s3['resource'])
    else:
        ds = netCDF4.Dataset(file_path, mode, format)
    return ds


def sd_wrapper(file_path):
    ds = None
    if 's3://' == file_path[0:5]:
        s3 = parse_s3_url(file_path)
        s3_client = boto3.client('s3')
        ds, tmpdir, full_filename = hdf4_dataset_from_s3(s3_client, s3['bucket_name'],
                                                        s3['prefix'] + s3['prefix_end'] + s3['resource'],
                                                        filename=s3['resource'])
    else:
        ds = pyhdf.SD.SD(file_path)
    return ds
