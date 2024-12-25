from modules.logger import logger
from modules.config import servers
from modules import helper
from modules import config

def test():
    print(config.config_data)
    return
    logger.info("Init")
    # helper.print_dict(servers)

    # check if previous rebuild is complete, if not exit this process.
    helper.check_if_rebuild_is_complete(config.config_data['lock_file'])

    # get the last rebuilt server and check if there is replication lag due to previous rebuild server,
    # if there is a lag exit else start rebuild.
    last_rebuild_server = helper.get_last_rebuilt_server(config.config_data['rebuild_status_file'])
    print('last_rebuild_server', last_rebuild_server, end="\n\n")

    # HARDCODED last_rebuild_server
    last_rebuild_server = '192.168.50.168:27018' # shard 2 host 1

    helper.check_replication_lag_of_previously_build_server(last_rebuild_server)

    return
    server_disk_used_map = helper.get_max_disk_used_server(servers)
    print('server_disk_used_map', server_disk_used_map, end="\n\n")


def main():
    pass


if __name__ == "__main__":
    # main()
    test()