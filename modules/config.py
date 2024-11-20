import yaml
from .logger import logger

def load_config():
    with open("configs/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    return config


config_data = load_config()

def load_servers_info():
    servers = {}
    cluster_name = config_data['cluster']

    for shard_name, shard_data in config_data['shard'].items():
        for host_key, host_data in shard_data.items():
            hostname = host_data['hostname']
            server_key = f"{hostname}"
            servers[server_key] = {
                "storage_path": host_data["storage_path"],
                "server_hardware_configuration": host_data["server_hardware_configuration"],
                "service_name": host_data["service_name"],
                "shard_name": shard_name,
                "ip": host_data["ip"],
                "cluster_name": cluster_name
            }

    logger.info('Loaded server details from the config.yaml file.')
    return servers

servers = load_servers_info()

shards = list(config_data['shard'].keys())