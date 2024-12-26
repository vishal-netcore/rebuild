import time

from modules.logger import logger
from modules.config import servers
from modules import helper
from modules import config
from modules import mongo


def test():
    print()
    # helper.print_dict(servers)
    logger.info("Init")

    '''check if previous rebuild is complete, if not exit this process.'''
    helper.check_if_rebuild_is_complete(config.config_data['lock_file'])

    '''get the last rebuilt server and check the status'''
    '''if still in STARTUP2 exit else proceed.'''
    # last_rebuild_server = helper.get_last_rebuilt_server(config.config_data['rebuild_status_file'])

    '''HARDCODED last_rebuild_server'''
    last_rebuild_server = '192.168.50.168:27018'  # shard 2 host 1
    helper.check_replication_lag_of_previously_build_server(last_rebuild_server, servers)

    '''get list of servers in descending order by their disk usage'''
    server_disk_used_map = helper.get_max_disk_used_server(servers)

    '''choose the server for rebuilding which has no replication lag and has more disk usage.'''
    max_disk_used_server = None
    for server, disk_used in server_disk_used_map.items():
        result = helper.check_replication_lag_of_shard(server, servers)

        if not result:
            logger.info(
                f'Server {server} with disk usage {disk_used} is experiencing replication lag, checking for the next server.')
        else:
            max_disk_used_server = server
            break

    logger.info(f'Starting rebuild of {max_disk_used_server}.')
    helper.update_rebuild_status(config.config_data['rebuild_status_file'], "started", max_disk_used_server)

    username = servers[max_disk_used_server]['username']
    password = servers[max_disk_used_server]['password']
    host = max_disk_used_server.split(':')[0]
    port = int(max_disk_used_server.split(':')[1])

    client = mongo.connect(username, password, host, port)

    '''update the servers list with replication members info'''
    helper.update_servers_dict(username, password, host, port, servers)
    max_disk_used_server_id = helper.get_id_by_server_name(servers, max_disk_used_server)

    print('max_disk_used_server_id', max_disk_used_server_id)

    '''Make the primary as secondary and other secondary high configuration server as primary.'''
    # helper.change_priority(servers, client, max_disk_used_server_id)
    # logger.info('Waiting for priority change to get updated.')
    wait_time = 30  # seconds
    time.sleep(wait_time)

    '''resize oplog to 400000mb.'''
    # helper.resize_oplog(client, max_disk_used_server)

    '''shutdown mongodb'''
    # helper.shutdown_mongodb(servers, max_disk_used_server)


def main():
    pass


if __name__ == "__main__":
    # main()
    test()
