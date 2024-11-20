from modules.logger import logger
from modules.config import servers
from modules import helper

def test():
    logger.info("Init")
    logger.info("testing")
    # helper.print_dict(servers)

    server_disk_used_map = helper.get_max_disk_used_server(servers)
    print(server_disk_used_map)

def main():
    pass


if __name__ == "__main__":
    # main()
    test()