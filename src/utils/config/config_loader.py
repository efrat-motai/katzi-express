import logging
from pathlib import Path
import yaml


def load_config():
    config_path = Path(__file__).resolve().parents[3]/"config"/"config.yml"
    config = yaml.safe_load(open(config_path))
    logging.basicConfig(level=config["logging"]["level"], format=config["logging"]["format"])
    logging.getLogger('pika').setLevel(logging.CRITICAL)
    logging.getLogger('kafka').setLevel(logging.WARNING)
    return config