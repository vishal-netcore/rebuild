import logging
import yaml

def load_path():
    with open("configs/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    return config.get("log_path", "/logs/rebuild.log")

logging.basicConfig(
    filename=load_path(),
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger()