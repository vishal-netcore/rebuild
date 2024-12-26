import subprocess
from .logger import logger
from .mongo import connect
from .config import shards, config_data
import time
import os
import gzip
from datetime import datetime, timedelta
import fcntl
import sys


def print_dict(d, indent=0):
    for key, value in d.items():
        if isinstance(value, dict):
            print('  ' * indent + f"{key}: {{")
            print_dict(value, indent + 1)
            print('  ' * indent + '}')
        else:
            print('  ' * indent + f"{key}: {value}")


def convert_to_gb(size_str):
    size, unit = size_str[:-1], size_str[-1]
    size = float(size)
    if unit == 'G':
        return size
    elif unit == 'M':
        return size / 1024
    elif unit == 'K':
        return size / (1024 * 1024)
    elif unit == 'T':
        return size * 1024


def execute_shell_command(servers, p_command, hostname):
    ip = servers[hostname]['ip']
    password = 'qwerasdf'
    command = ["sshpass", f"-p {password}", "ssh", f"{ip}"] + p_command
    result = subprocess.run(' '.join(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
    return result.stdout


def execute_docker_command(container_name, p_command):
    command = ['docker', 'exec', container_name] + p_command

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode != 0:
            raise Exception(f"Error executing command: {result.stderr}")

        return result.stdout
    except Exception as e:
        return f"An error occurred: {str(e)}"


def get_max_disk_used_server(servers):
    try:
        max_disk_space = float("-inf")

        command = ['du', '-sh']
        server_disk_used_map = {}

        for server, info in servers.items():
            path = info["storage_path"]
            command.append(path)
            utilization_string = execute_shell_command(servers, command, server)
            # utilization_string = execute_docker_command(servers[server]['ip'], command)
            # utilization_string = utilization_string.decode('utf-8')
            utilization = convert_to_gb(utilization_string.split('\t')[0])
            server_disk_used_map[server] = utilization

            if utilization > max_disk_space:
                max_disk_space = utilization

            command.pop(-1)

        server_disk_used_map = dict(sorted(server_disk_used_map.items(), key=lambda x: x[1], reverse=True))
    except Exception as e:
        logger.error(f"Error while getting max disk used server: {e}")
        exit(1)

    return server_disk_used_map


def calculate_replication_lag(primary_optime, secondary_optime):
    lag = primary_optime - secondary_optime
    return lag.total_seconds() * 1000


def check_replication_lag_across_cluster(servers):
    try:
        shard_servers_map = {elem: [] for elem in shards}

        for server, info in servers.items():
            shard_servers_map[info['shard_name']].append(server)

        for _, pss_list in shard_servers_map.items():
            tmp_client = connect(username, password, pss_list[0], port, False)
            # tmp_client = MongoClient(f'mongodb://root:jazzychess342@{pss_list[0]}')
            replica_set_status = tmp_client.admin.command("replSetGetStatus")

            primary_optime = None
            for member in replica_set_status["members"]:
                if member["stateStr"] == "PRIMARY":
                    primary_optime = member["optimeDate"]
                    break

            for member in replica_set_status["members"]:
                if member["stateStr"] == "SECONDARY":
                    secondary_optime = member["optimeDate"]
                    lag = calculate_replication_lag(primary_optime, secondary_optime)
                    print(f'replication lag for {member["name"]}: {lag}')
                    if lag != 0.0:
                        return False
    except Exception as e:
        logger.error(f"Error while checking replication lag across cluster: {e}")
        exit(1)
    return True


def start_mongodb(servers, max_disk_used_server):
    try:
        service_name = servers[max_disk_used_server]['service_name']
        execute_shell_command(['systemctl', 'start', f'{service_name}.service'], max_disk_used_server)
    except Exception as e:
        logger.error(f'error while starting service {service_name}, {e}')
        # print(f"Error: {e}")
    logger.info(f'Started MongoDB on server {max_disk_used_server}, service: {service_name}.')


def set_sync_from(servers, max_disk_used_server):  # args:
    '''
    execute from re-built server
    looks for secondary server and sets sync from
    '''
    try:
        # tmp_client = MongoClient(f'mongodb://root:jazzychess342@{max_disk_used_server}/?directConnection=true')
        tmp_client = connect(username, password, max_disk_used_server, port, True)

        while True:
            if servers[max_disk_used_server]['syncSourceHost'] != '':
                if servers[servers[max_disk_used_server]['syncSourceHost']]['stateStr'] == 'PRIMARY':
                    primary_server = servers[servers[max_disk_used_server]['syncSourceHost']]['name']

                    for server, info in servers.items():
                        if server != max_disk_used_server and (info['syncSourceHost'] == primary_server or info[
                            'syncSourceHost'] == max_disk_used_server):
                            # print(server)
                            tmp_client.admin.command('replSetSyncFrom', server)
                            logger.info(f'Setting sync source for {max_disk_used_server} to {server}.')
                            return server
                else:
                    server = servers[servers[max_disk_used_server]['syncSourceHost']]['name']
                    # print(server)
                    tmp_client.admin.command('replSetSyncFrom', server)
                    logger.info(f'Setting sync source for {max_disk_used_server} to {server}.')
                    return server
            else:
                logger.info('Sync source host is empty, waiting for 3 seconds.')
                connect(max_disk_used_server)
                time.sleep(3)
    except Exception as e:
        logger.error(f"Error while setting sync from for {max_disk_used_server}: {e}")
        exit(1)


def delete_directory(servers, max_disk_used_server):  # args : server_ip/id
    # also verify it
    try:
        res = execute_shell_command(['sudo', 'rm', '-rf', f'{servers[max_disk_used_server]["storage_path"]}'],
                                    max_disk_used_server)
        res = execute_shell_command(['sudo', 'mkdir', f'{servers[max_disk_used_server]["storage_path"]}'],
                                    max_disk_used_server)
        res = execute_shell_command(
            ['sudo', 'chown', '-R', 'mongod:mongod', f'{servers[max_disk_used_server]["storage_path"]}'],
            max_disk_used_server)
    except Exception as e:
        logger.error(f'error while deleting directory {servers[max_disk_used_server]["storage_path"]}, {e}')
        # print(f'error while deleting directory {servers[max_disk_used_server]["storage_path"]}, {e}')
        exit(1)

    logger.info(f'Deleted directory: {servers[max_disk_used_server]["storage_path"]}.')
    # logger.info(f'Deleted directory /database/mongocluster/mongodata2.')
    # if res[:4] == '4.0K':
    #     print('delete success')
    # else:
    #     print('delete failed: directory size is not equals to 4.0K')


# new and refactored functions below.
def check_if_rebuild_is_complete(lock_file):
    try:
        with open(lock_file, 'w') as lf:
            fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
            logger.info(f"Previous rebuild is complete, string new rebuild.")
            return

    except IOError:
        logger.info(f"Previous rebuild process is running. Exiting.")
        sys.exit(0)


def get_last_rebuilt_server(rebuild_status_file):
    # if file does not exist create the file.
    if not os.path.exists(rebuild_status_file):
        logger.info(f"{rebuild_status_file} does not exists, creating new file.")
        with open(rebuild_status_file, 'w') as _:
            return None

    # get the last rebuilt server
    last_rebuilt_server = None
    last_two_lines = []

    with open(rebuild_status_file, 'r') as file:
        lines = file.readlines()
        if lines:
            last_two_lines = lines[-2:]
            _, server_name, status = lines[-1].strip().split("__")
            if status == "completed":
                last_rebuilt_server = server_name

    # get size of the file, if more than 5mb take backup of the file compress it and delete the old file.
    if os.path.getsize(rebuild_status_file) > 5 * 1024 * 1024:
        logger.info(f"{rebuild_status_file} size has reached 5mb, taking backup and deleting.")
        backup_name = f"{rebuild_status_file}_bkp_{datetime.now().strftime('%Y%m%d%H%M%S')}.gz"
        with open(rebuild_status_file, 'rb') as f_in, gzip.open(backup_name, 'wb') as f_out:
            f_out.writelines(f_in)
        os.remove(rebuild_status_file)
        with open(rebuild_status_file, 'w') as new_file:
            new_file.writelines(last_two_lines)

    logger.info(f"Last rebuilt server: {last_rebuilt_server}")
    return last_rebuilt_server


def update_rebuild_status(rebuild_status_file, status, server_name):
    """
    updates the status in rebuild_status_file in current_datetime__server_name__status format
    """
    with open(rebuild_status_file, "a") as file:
        file.write(f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}__{server_name}__{status}\n")


def check_replication_lag_of_shard(server, servers):
    try:
        username = servers[server]['username']
        password = servers[server]['password']
        host = server.split(':')[0]
        port = int(server.split(':')[1])

        tmp_client = connect(username, password, host, port)  #(f'mongodb://root:jazzychess342@{mongo_host}')
        replica_set_status = tmp_client.admin.command("replSetGetStatus")

        primary_optime = None
        for member in replica_set_status["members"]:
            if member["stateStr"] == "PRIMARY":
                primary_optime = member["optimeDate"]
                break

        for member in replica_set_status["members"]:
            if member["stateStr"] == "SECONDARY":
                secondary_optime = member["optimeDate"]
                lag = calculate_replication_lag(primary_optime, secondary_optime)
                logger.info(f'Replication lag of {member["name"]} is {lag} seconds.')
                if lag > 5.0:
                    return False
    except Exception as e:
        logger.error(f"Error while checking replication lag of shard using {server}: {e}")
        exit(1)

    return True


def check_replication_lag_of_previously_build_server(last_rebuilt_server, servers):
    """
    process will terminate if replication lag is greater than 0.
    """
    # TODO: implement 3 retires
    try:
        if last_rebuilt_server == '':
            logger.info(f"No last rebuilt server exists, proceeding.")
            return

        # check if last rebuilt server is in STARTUP2 state
        # if in STARTUP2 state exit, else proceed.
        username = servers[last_rebuilt_server]['username']
        password = servers[last_rebuilt_server]['password']
        host = last_rebuilt_server.split(':')[0]
        port = int(last_rebuilt_server.split(':')[1])

        tmp_client = connect(username, password, host, port)  #(f'mongodb://root:root@{last_rebuilt_server}')
        replica_set_status = tmp_client.admin.command("replSetGetStatus")
        members = replica_set_status["members"]

        for member in members:
            if member['stateStr'] == 'STARTUP2':
                logger.info(f'Last rebuilt server {last_rebuilt_server} is in "STARTUP2" state, terminating.')
                sys.exit(0)
    except Exception as e:
        logger.error(f"Error while checking replication lag of previously rebuilt server: {e}")
        exit(1)

    # check replication lag of the shard
    # if lag is zero proceed, else exit.
    res = check_replication_lag_of_shard(last_rebuilt_server, servers)
    if res:
        logger.info(f'Replication lag of previously rebuilt server {last_rebuilt_server} is zero, proceeding.')
        return
    else:
        logger.info(f'Replication lag of previously rebuilt server {last_rebuilt_server} is not zero, terminating...')
        exit(1)


def get_id_by_server_name(servers, target_server):
    for key, val in servers.items():
        if target_server in key:
            return val['_id']


def update_servers_dict(username, password, host, port, servers):
    client = connect(username, password, host, port)
    replica_set_status = client.admin.command("replSetGetStatus")
    members = replica_set_status["members"]

    for member in members:
        servers[member['name']].update(member)


def change_priority(servers, client, target_id):
    """
    Adjusts the replication set priorities for MongoDB members based on their hardware configurations.
    """
    try:
        db = client.admin
        config = db.command('replSetGetConfig')
        status_members = db.command("replSetGetStatus")['members']
        config_members = config['config']['members']

        # Merge stateStr from replSetGetStatus into config_members
        for config_member in config_members:
            for status_member in status_members:
                if status_member['name'] == config_member['host']:
                    config_member['stateStr'] = status_member['stateStr']

        # Merge hardware configuration from servers dictionary into config_members
        for server_name, server_info in servers.items():
            for config_member in config_members:
                if config_member['host'] == server_name:
                    config_member['server_hardware_configuration'] = server_info['server_hardware_configuration']

        # Adjust priorities based on hardware configuration
        high_hardware = []
        low_hardware = []

        for member in config_members:
            if member["_id"] == target_id:
                member["priority"] = 1
            else:
                if member["server_hardware_configuration"] == "HIGH":
                    high_hardware.append(member)
                else:
                    low_hardware.append(member)

        # Assign priorities
        if len(high_hardware) == 2:
            high_hardware[0]["priority"] = 5
            high_hardware[1]["priority"] = 4
        elif len(high_hardware) == 1 and len(low_hardware) == 1:
            high_hardware[0]["priority"] = 5
            low_hardware[0]["priority"] = 4

        # Cleanup additional attributes
        for config_member in config_members:
            config_member.pop("stateStr", None)
            config_member.pop("server_hardware_configuration", None)

        # Apply the updated configuration
        config['config']['members'] = config_members
        db.command({'replSetReconfig': config['config'], 'force': True})
        logger.info("Changed priorities.")
    except Exception as e:
        logger.info(f"Error: {e}")


def resize_oplog(client, max_disk_used_server, oplog_size=400000.0):
    try:
        admin_db = client.admin
        admin_db.command({"replSetResizeOplog": 1, "size": oplog_size})
        logger.info(f'Resized oplog size to {oplog_size}MB.')
    except Exception as e:
        logger.error(f"Error while resizing oplog using {max_disk_used_server}: {e}")
        exit(1)


def shutdown_mongodb(servers, max_disk_used_server):
    service_name = None
    try:
        service_name = servers[max_disk_used_server]['service_name']
        print(service_name)
        exit()
        execute_shell_command(['systemctl', 'stop', f'{service_name}.service'], max_disk_used_server)
    except Exception as e:
        logger.error(f'error while stopping service {service_name}, {e}')
        # print(f"Error: {e}")
    logger.info(f'Stopped MongoDB service, service: {service_name}.')

