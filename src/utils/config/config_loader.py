import logging
import os

import yaml


def load_config():
    current_file_path = os.path.abspath(__file__)
    utils_dir = os.path.dirname(current_file_path)
    config_path = os.path.join(utils_dir, "..", "..", "config", "config.yml")
    config_path = os.path.normpath(config_path)
    config = yaml.safe_load(open(config_path))
    logging.basicConfig(level=config["logging"]["level"], format=config["logging"]["format"])
    logging.getLogger('pika').setLevel(logging.CRITICAL)
    logging.getLogger('kafka').setLevel(logging.WARNING)
    return config


