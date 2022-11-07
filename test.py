import pprint
from config_tool import load_config, flatten_config, config_usage_to_dict

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


if __name__ == "__main__":
    config = main()
    print(config.b.b2)
    pprint.pprint(config_usage_to_dict(config, "count"))