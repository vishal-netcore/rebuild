from modules.logger import logger
from modules.config import servers
from modules import helper
from modules import config

def test():
    logger.info("Init")
    # helper.print_dict(servers)

    # check if previous rebuild is complete, if not exit this process.
    helper.check_if_rebuild_is_complete(config.config_data['lock_file'])

    # get the last rebuilt server and check if op
    last_rebuild_server = helper.get_last_rebuilt_server(config.config_data['rebuild_status_file'])
    print('last_rebuild_server', last_rebuild_server)
    return

    server_disk_used_map = helper.get_max_disk_used_server(servers)
    print('server_disk_used_map', server_disk_used_map)


def main():
    pass


if __name__ == "__main__":
    # main()
    test()