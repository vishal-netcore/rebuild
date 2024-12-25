import logging
import yaml
from datetime import datetime

def load_path():
    with open("configs/config.yaml", "r") as file:
        config = yaml.safe_load(file)
        log_path = config.get("log_path", "/logs/rebuild.log")
        log_path += "rebuild_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"
        print('log__path', log_path)
        return log_path

logging.basicConfig(
    filename=load_path() ,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger()