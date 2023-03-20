'''
MIT License

Copyright (c) 2022 Jia-Qi Yang

https://github.com/ThyrixYang/config_tool

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import os
import re
import yaml
import pprint
import inspect
import fsspec
from copy import deepcopy
from collections.abc import Mapping
from .utils import deep_update, load_yaml, deep_filter


def config_to_dict(x):
    if isinstance(x, Config):
        res = {}
        for k, v in x._config_dict.items():
            res[k] = config_to_dict(v)
        return res
    else:
        return x

def config_usage_to_dict(x, key):
    if isinstance(x, Config):
        res = {}
        for k, v in x._config_dict.items():
            res[k] = config_usage_to_dict(v, key)
        if hasattr(x, "_usage_state"):
            for kk, vv in x._usage_state.items():
                res[kk] = vv[key]
        return res
    else:
        return {}


def flatten_config(c, prefix=""):
    res = {}
    for k, v in c._config_dict.items():
        if prefix == "":
            name = k
        else:
            name = prefix + "." + k
        if isinstance(v, Config):
            res.update(flatten_config(v, name))
        else:
            res[name] = v
    return Config(res)

def recover_flattened_config(config):
    config_dict = config_to_dict(config)
    recovered_dict = {}
    for k, v in config_dict.items():
        ks = k.split(".")
        _vv = v
        for _k in reversed(ks):
            res_dict = {_k: _vv}
            _vv = res_dict
        recovered_dict = deep_update(recovered_dict, res_dict)
    return Config(recovered_dict)


class Config(Mapping):

    def __init__(self,
                 config_dict={},
                 usage_state_level="count"):
        self.reset_config(config_dict=config_dict,
                          usage_state_level=usage_state_level)
        
    def copy(self):
        return Config(deepcopy(config_to_dict(self)))

    def reset_config(self, config_dict, usage_state_level="count"):
        assert usage_state_level in ["none", "count", "hist"]
        self._config_dict = {}
        self._usage_state = {}
        self._usage_state_level = usage_state_level
        for k in config_dict.keys():
            if isinstance(config_dict[k], dict):
                self._config_dict[k] = Config(config_dict[k],
                                              usage_state_level=usage_state_level)
            else:
                self._config_dict[k] = config_dict[k]
                self._usage_state[k] = {"count": 0, "hist": []}

    def __getattr__(self, key):
        if key in ["_config_dict", "_usage_state_level", "_usage_state"]:
            return super().__getattr__(key)
        return self.__getitem__(key)
    
    def __setattr__(self, key, value):
        if key in ["_config_dict", "_usage_state_level", "_usage_state"]:
            super().__setattr__(key, value)
            return
        self.__setitem__(key, value)

    def __getitem__(self, key):
        if key not in self._config_dict:
            raise ValueError("Key '{}' not in config: {}".format(key, self))
        if not isinstance(self._config_dict[key], Config):
            if self._usage_state_level == "none":
                pass
            elif self._usage_state_level == "count":
                self._usage_state[key]["count"] += 1
            elif self._usage_state_level == "hist":
                self._usage_state[key]["count"] += 1
                s = [(f"filename: {x.filename}, line: {x.lineno}, code: {x.code_context}")
                     for x in inspect.stack()[1:]]
                self._usage_state[key]["hist"].append(s)
            else:
                raise ValueError()
        return self._config_dict[key]
    
    def __setitem__(self, key, value):
        self._config_dict[key] = value
        if self._usage_state_level == "none":
            pass
        elif self._usage_state_level == "count":
            self._usage_state[key] = {"count": 1}
        elif self._usage_state_level == "hist":
            self._usage_state[key] = {"count": 0, "hist": []}
        else:
            raise ValueError()

    def __iter__(self):
        for k in self._config_dict.keys():
            yield k

    def __len__(self):
        return len(self._config_dict)

    def pprint(self):
        d = config_to_dict(self)
        pprint.pprint(d)

    def __getstate__(self):
        state = config_to_dict(self)
        return state

    def __setstate__(self, state):
        self.reset_config(state, usage_state_level="none")

    def to_file(self, path):
        # dir_path = os.path.dirname(path)
        # os.makedirs(dir_path, exist_ok=True)
        with fsspec.open(path, "w", auto_mkdir=True) as f:
            yaml.dump(
                config_to_dict(self),
                f,
                default_flow_style=False)
            
    def __eq__(self, other):
        for k in self._config_dict.keys():
            if k not in list(other._config_dict.keys()):
                return False
        for k in other._config_dict.keys():
            if k not in list(self._config_dict.keys()):
                return False
        for k in self._config_dict.keys():
            if not self._config_dict[k] == other._config_dict[k]:
                return False
        return True

    def __str__(self):
        return str(config_to_dict(self))

    def __repr__(self):
        return self.__str__()


def _load_config(file_path):
    return load_yaml(file_path + ".yaml")

def check_config_path(file_path):
    num_sub = file_path.count("-")
    assert num_sub <= 1
    if num_sub == 1:
        pos_sub = file_path.rfind("-")
        pos_add = file_path.rfind("+")
        assert pos_add < pos_sub
    return True


def load_config(file_path, usage_state_level="hist"):
    prefix = "/".join(file_path.split("/")[:-1])
    file_path = file_path.split("/")[-1]

    check_config_path(file_path)
    if file_path.endswith(".yaml"):
        file_path = file_path.replace(".yaml", "")
    file_paths = file_path.split("-")
    if len(file_paths) > 1:
        assert len(file_paths) == 2
        sub_path = file_paths[1]
    else:
        sub_path = None
    file_path = file_paths[0]
    file_paths = file_path.split("+")
    config = {}
    for i in range(len(file_paths)):
        _config = _load_config(os.path.join(prefix, file_paths[i]))
        config = deep_update(config, _config)
    if sub_path is not None:
        _config = _load_config(os.path.join(prefix, sub_path))
        config = deep_filter(config, _config)
    return Config(config, usage_state_level=usage_state_level)