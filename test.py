import pprint
import fsspec
import numpy as np
from rena.config import *


def main():
    config = load_config("configs/test+t-std.yaml")
    config.pprint()
    flat_config = flatten_config(config)
    print(flat_config)
    print(config.a.a1.a12)
    print(config.a.a1.a12)
    print(config.b.b3.b31.b41)
    pprint.pprint(config_usage_to_dict(config, "count"))
    pprint.pprint(config_usage_to_dict(config, "hist"))
    return config


def test_new():
    config = load_config("configs/test+t-std.yaml")
    print(config)
    flat_config = flatten_config(config)
    print(flat_config)
    print(recover_flattened_config(flat_config))
    config2 = config.copy()
    config2.a1 = 2
    print(config2)
    print(config == config2)
    exit()
    print(config.a.a1.a12)
    print(config.a.a1.a12)
    print(config.b.b3.b31.b41)
    print(config)

    def fn(a, b):
        print(a)
        print(b)
        return
    fn(**config)
    # pprint.pprint(config_usage_to_dict(config, "count"))
    # pprint.pprint(config_usage_to_dict(config, "hist"))
    return config

def gen_config():
    generate_random_config_search("configs/std", 
                                  "configs/std_search", 
                                  "s3://tmp/test_config_tool/configs/tmp", 
                                  check_duplicate=True,
                                  num=10)

import time

def test_fn(config):
    time.sleep(random.randint(10, 20))
    return {
        "metrics": {"loss": int(np.random.rand())}
    }

def test_parameter_search():
    # generate_random_config_search("configs/std", 
    #                               "configs/std_search", 
    #                               "s3://tmp/test_config_tool/configs/tmp", 
    #                               check_duplicate=True,
    #                               num=100)
    clear_locks("s3://tmp/test_config_tool/configs/tmp")
    # exit()
    launch_search(test_fn, "s3://tmp/test_config_tool/configs/tmp")

if __name__ == "__main__":
    # config = main()
    # print(config.b.b2)
    # pprint.pprint(config_usage_to_dict(config, "count"))
    # test_new()
    # gen_config()
    # print(fsspec.core.url_to_fs("s3://tmp"))
    # res = rfs_ls("s3://tmp/test_config_tool/configs/tmp")
    test_parameter_search()