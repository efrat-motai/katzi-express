import logging

import yaml


def load_config(config_path="../../config/config.yml"):
    config = yaml.safe_load(open(config_path))
    logging.basicConfig(level=config["logging"]["level"], format=config["logging"]["format"])
    logging.getLogger('pika').setLevel(logging.WARNING)
    logging.getLogger('kafka').setLevel(logging.WARNING)
    return config