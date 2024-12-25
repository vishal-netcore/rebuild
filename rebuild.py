from modules.logger import logger
from modules.config import servers
from modules import helper

def test():
    logger.info("Init")
    # helper.print_dict(servers)

    server_disk_used_map = helper.get_max_disk_used_server(servers)
    print(server_disk_used_map)

    last_rebuild_server = helper.preprocessing()

    print('last_rebuild_server', last_rebuild_server)
    print()

def main():
    pass


if __name__ == "__main__":
    # main()
    test()