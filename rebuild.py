from modules.logger import logger
from modules.config import servers
from modules import helper
from modules import config
from modules import mongo

def test():
    # helper.print_dict(servers)
    logger.info("Init")

    # check if previous rebuild is complete, if not exit this process.
    helper.check_if_rebuild_is_complete(config.config_data['lock_file'])

    # get the last rebuilt server and check if there is replication lag due to previous rebuild server,
    # if there is a lag exit else start rebuild.
    # last_rebuild_server = helper.get_last_rebuilt_server(config.config_data['rebuild_status_file'])
    # print('last_rebuild_server', last_rebuild_server, end="\n\n")

    # HARDCODED last_rebuild_server
    last_rebuild_server = '192.168.50.168:27018' # shard 2 host 1

    helper.check_replication_lag_of_previously_build_server(last_rebuild_server, servers)

    server_disk_used_map = helper.get_max_disk_used_server(servers)

    max_disk_used_server = None
    for server, disk_used in server_disk_used_map.items():
        result = helper.check_replication_lag_of_shard(server ,servers)

        if not result:
            logger.info(f'Server {server} with disk usage {disk_used} is experiencing replication lag, checking for the next server.')
        else:
            max_disk_used_server = server
            break

    print('max_disk_used_server', max_disk_used_server, end="\n\n")
    logger.info(f'Starting rebuild of {max_disk_used_server}.')

    username = servers[max_disk_used_server]['username']
    password = servers[max_disk_used_server]['password']
    host = max_disk_used_server.split(':')[0]
    port = int(max_disk_used_server.split(':')[1])

    client = mongo.connect(username, password, host, port)
    helper.update_servers_dict(username, password, host, port, servers)
    max_disk_used_server_id = helper.get_id_by_server_name(servers, max_disk_used_server)
    print('max_disk_used_server_id', max_disk_used_server_id)


def main():
    pass


if __name__ == "__main__":
    # main()
    test()