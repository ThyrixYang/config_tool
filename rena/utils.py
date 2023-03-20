import yaml
import re
import fsspec
import paramiko

def get_ssh_client(hostname, username):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(hostname=hostname, username=username)
    return ssh

def get_central_dt(hostname, username):
    ssh = get_ssh_client(hostname, username)
    stdin, stdout, stderr = ssh.exec_command(
        "echo $(date '+%Y/%m/%d_%H:%M:%S')")
    dt = stdout.readlines()[0].strip()
    ssh.close()
    return dt

def get_dt_for_file_name():
    dt = get_central_dt("114.212.23.229", "yangjq")
    return dt.replace(":", "_")

def deep_update(mapping, *updating_mappings):
    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if k in updated_mapping and isinstance(updated_mapping[k], dict) and isinstance(v, dict):
                updated_mapping[k] = deep_update(updated_mapping[k], v)
            else:
                updated_mapping[k] = v
    return updated_mapping


def deep_filter(mapping, selector):
    new_mapping = {}
    for k, v in selector.items():
        if k in mapping and isinstance(mapping[k], dict) and isinstance(v, dict):
            new_mapping[k] = deep_filter(mapping[k], v)
        else:
            new_mapping[k] = mapping[k]
    return new_mapping


loader = yaml.SafeLoader
loader.add_implicit_resolver(
    u'tag:yaml.org,2002:float',
    re.compile(u'''^(?:
     [-+]?(?:[0-9][0-9_]*)\\.[0-9_]*(?:[eE][-+]?[0-9]+)?
    |[-+]?(?:[0-9][0-9_]*)(?:[eE][-+]?[0-9]+)
    |\\.[0-9_]+(?:[eE][-+][0-9]+)?
    |[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\\.[0-9_]*
    |[-+]?\\.(?:inf|Inf|INF)
    |\\.(?:nan|NaN|NAN))$''', re.X),
    list(u'-+0123456789.'))

def load_yaml(file_path):
    with fsspec.open(file_path, "r") as f:
        return yaml.load(f, Loader=loader)
    
def save_yaml(obj, file_path):
    with fsspec.open(file_path, "w") as f:
        yaml.dump(
            obj,
            f,
            default_flow_style=False)
    