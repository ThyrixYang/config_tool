import fsspec

def isfile(url, *, fs=None):
    if fs is not None:
        return fs.isfile(url_path)
    fs, url_path = fsspec.core.url_to_fs(url)
    return fs.isfile(url_path)
def ls(url, *, fs=None):
    if fs is not None:
        res = fs.ls(url_path, detail=False)
    else:
        fs, url_path = fsspec.core.url_to_fs(url)
        res = fs.ls(url_path, detail=False)
    if url.startswith("s3://"):
        res = ["s3://" + r for r in res]
    return res
    
def rm(url, recursive=False, *, fs=None):
    if fs is not None:
        fs.rm(url_path, recursive=recursive)
    else:
        fs, url_path = fsspec.core.url_to_fs(url)
        fs.rm(url_path, recursive=recursive)
        
def put(local_path, remote_path, recursive=True):
    fs = fsspec.core.url_to_fs(remote_path)
    fs.put(local_path, remote_path, recursive=recursive)