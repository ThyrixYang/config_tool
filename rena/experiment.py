import os
import random
import fsspec
import pickle
import time
import shutil
from functools import wraps
from copy import deepcopy

from .config import load_config, config_to_dict, flatten_config, recover_flattened_config, Config
from .utils import save_yaml, load_yaml, get_dt_for_file_name
import rfs
import dist

@dist.rank_zero_only
def require_global_lock():
    while True:
        print("Waiting for global lock")
        time.sleep(random.random()*5 + 5)
        if not rfs.isfile(dist._global_lock_file):
            with fsspec.open(dist._global_lock_file, "w") as f:
                f.write("Locked")
            break
        else:
            continue
    return

@dist.rank_zero_only
def release_global_lock():
    assert rfs.isfile(dist._global_lock_file)
    rfs.rm(dist._global_lock_file)
    print("global lock released")

def get_file_id(file_name_func):
    i = 0
    while(rfs.isfile(file_name_func(i))):
        i += 1
    return i

def generate_random_config_search(base_config_path,
                                  search_config_path,
                                  prefix,
                                  num,
                                  check_duplicate=True):
    
    base_config = load_config(base_config_path)
    search_config = load_config(search_config_path)
    flat_base_config = config_to_dict(flatten_config(base_config))
    flat_search_config = config_to_dict(flatten_config(search_config))
    
    def file_name_func(i):
        return os.path.join(prefix, "{}".format(i), "config.yaml")
    
    start_id = get_file_id(file_name_func)
    existed_configs = [load_config(file_name_func(i)) for i in range(start_id)]
    
    file_id = start_id
    cnt = 0
    for _ in range(start_id, start_id + num):
        _flat_base_config = deepcopy(flat_base_config)
        for k in flat_search_config.keys():
            assert isinstance(flat_search_config[k], list)
            assert k in flat_base_config
            _flat_base_config[k] = random.choice(flat_search_config[k])
        _res_config = recover_flattened_config(Config(_flat_base_config))
        if check_duplicate:
            found = False
            for c in existed_configs:
                if c == _res_config:
                    found = True
                    break
            if found:
                continue
        existed_configs.append(_res_config)
        file_save_path = file_name_func(file_id)
        _res_config.to_file(file_save_path)
        file_id += 1
        cnt += 1
    print("Generated {} configs".format(cnt))

def get_config_file_path(config_dir):
    return os.path.join(config_dir, "config.yaml")

def get_finished_tag_path(config_dir):
    return os.path.join(config_dir, "finished.tag")
    
def get_lock_tag_path(config_dir):
    return os.path.join(config_dir, "lock.tag")

def check_finished(config_dir):
    config_path = get_config_file_path(config_dir)
    lock_path = get_lock_tag_path(config_dir)
    finish_tag_path = get_finished_tag_path(config_dir)
    return rfs.isfile(finish_tag_path) \
        and not rfs.isfile(lock_path) \
            and rfs.isfile(config_path)

def check_todo(config_dir):
    config_path = get_config_file_path(config_dir)
    lock_path = get_lock_tag_path(config_dir)
    finish_tag_path = get_finished_tag_path(config_dir)
    return not rfs.isfile(lock_path) \
        and not rfs.isfile(finish_tag_path) \
            and rfs.isfile(config_path)
    
    
def get_config_dirs(prefix, mode="all", find_one=False):
    config_dirs = rfs.ls(prefix)
    res_config_dirs = []
    for config_dir in config_dirs:
        config_path = get_config_file_path(config_dir)
        if not rfs.isfile(config_path):
            continue
        if mode == "all":
            if find_one:
                return config_dir
            res_config_dirs.append(config_dir)
        elif mode == "finished":
            if check_finished(config_dir):
                if find_one:
                    return config_dir
                res_config_dirs.append(config_dir)
        elif mode == "todo":
            if check_todo(config_dir):
                if find_one:
                    return config_dir
                res_config_dirs.append(config_dir)
        elif mode == "locked":
            if rfs.isfile(get_lock_tag_path(config_dir)):
                if find_one:
                    return config_dir
                res_config_dirs.append(config_dir)
        else:
            raise ValueError()
    if find_one:
        return None
    else:
        return res_config_dirs

@dist.rank_zero_only
def clear_locks(prefix):
    locked_dirs = get_config_dirs(prefix, mode="locked")
    print("Locked dirs: {}".format(locked_dirs))
    cmd = input("Clear locks?: (Y)")
    if cmd == "Y":
        for config_dir in locked_dirs:
            rfs.rm(get_lock_tag_path(config_dir))
        print("Unlocked")
    else:
        print("Keep lock")
    
def remote_exp_func(fn, base_folder):
    
    @wraps(fn)
    def wrapped_fn(*args, **kwargs):
        os.makedirs(base_folder, exist_ok=True)
        results = fn(*args, **kwargs)
        assert isinstance(results, dict) and isinstance(results.get("metrics"), dict)
        results_path = os.path.join(base_folder, "results.pkl")
        metrics_path = os.path.join(base_folder, "metrics.yaml")
        with fsspec.open(results_path, "wb") as f:
            pickle.dump(results, f)
        save_yaml(results["metrics"], metrics_path)
        if "persistent_dir" in results:
            persistent_dir_path = os.path.join("s3://tmp/persistent_dirs/",
                                               get_dt_for_file_name())
            local_dir_path = results["persistent_dir"]
            rfs.put(local_dir_path, persistent_dir_path, recursive=True)
            shutil.rmtree(local_dir_path)
            persistent_dir_link = os.path.join(base_folder, "persistent_dir_link.txt")
            with fsspec.open(persistent_dir_link, "w") as f:
                f.write(persistent_dir_path)
            print("Persistent dir uploaded, local files deleted")
        return results

    return wrapped_fn

def launch_search(func, prefix):
    while True:
        require_global_lock()
        config_dir = get_config_dirs(prefix, mode="todo", find_one=True)
        if config_dir is None:
            print("No config_dir found")
            time.sleep(60)
            continue
        with fsspec.open(get_lock_tag_path(config_dir), "w") as f:
            f.write("Running")
        release_global_lock()
            
        print("Running config: {}".format(config_dir))
        config = load_config(get_config_file_path(config_dir))
        
        wrapped_func = remote_exp_func(func, config_dir)
        results = wrapped_func(config)
        
        # ##############
        # # save results
        # results_path = os.path.join(config_dir, "results.pkl")
        # metrics_path = os.path.join(config_dir, "metrics.yaml")
        # with fsspec.open(results_path, "wb") as f:
        #     pickle.dump(results, f)
        # save_yaml(results["metrics"], metrics_path)
        # if "persistent_dir" in results:
        #     persistent_dir_path = os.path.join("s3://tmp/persistent_dirs/",
        #                                        get_dt_for_file_name())
        #     local_dir_path = results["persistent_dir"]
        #     rfs.put(local_dir_path, persistent_dir_path, recursive=True)
        #     shutil.rmtree(local_dir_path)
        #     persistent_dir_link = os.path.join(config_dir, "persistent_dir_link.txt")
        #     with fsspec.open(persistent_dir_link, "w") as f:
        #         f.write(persistent_dir_path)
        #     print("Persistent dir uploaded, local files deleted")
        ##############
            
        rfs.rm(get_lock_tag_path(config_dir))
        with fsspec.open(get_finished_tag_path(config_dir), "w") as f:
            f.write("Finished")
            
def summarize_search(prefix):
    finished_config_dirs = get_config_dirs(prefix, mode="finished")
    for i, config_dir in enumerate(finished_config_dirs):
        metrics_path = os.path.join(config_dir, "metrics.yaml")
        config_path = os.path.join(config_dir, "config.yaml")
        metrics = load_yaml(metrics_path)
        config = load_yaml(config_path)
        print(i, metrics, config)