import subprocess
from .logger import logger
from .mongo import connect
from .config import shards, config_data
import time
import os 
from datetime import datetime, timedelta

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


def get_id_by_server_name(servers, target_server):
    for key, val in servers.items():
        if target_server in key:
            return val['_id']


def execute_shell_command(servers, p_command, hostname):
    ip=servers[hostname]['ip']
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


def check_replication_lag_of_shard(mongo_host):
    try:
        tmp_client = connect(username, password, host, port, False)
        # tmp_client = MongoClient(f'mongodb://root:jazzychess342@{mongo_host}')
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
                logger.info(f'Replication lag for {member["name"]}: {lag} seconds.')
                if lag != 0.0:
                    return False
    except Exception as e:
        logger.error(f"Error while checking replication lag of shard from {mongo_host}: {e}")
        exit(1)

    return True


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


def swap_priority(client, target_id): #target_id(server which needs to rebuilt/server having max disk usase)
    '''
    get the member with lowest priority
    and swap priority with it
    after swapping target will have lowest priority and ready to rebuild 
    '''
    try:
        db = client.admin
        config = db.command('replSetGetConfig')

        config_members = config['config']['members']

        # print('config_members', config_members)

        lowest_priority_member = None
        lowest_priority = float('inf')

        for member in config_members:
            if member['priority'] < lowest_priority:
                lowest_priority = member['priority']
                lowest_priority_member = member
            if member['_id'] == target_id:
                target_member = member
        
        lowest_priority_member['priority'], target_member['priority'] = target_member['priority'], lowest_priority_member['priority'] 
        logger.info(f'Swapping priority between {lowest_priority_member["host"]} and {target_member["host"]}.')

        # for member in config_members:
        #     print("ID:", member['_id'], "Priority:", member['priority'])
    except Exception as e:
        logger.error(f"Error while swapping priority: {e}")
        exit(1)

    try:
        result = db.command('replSetReconfig', config['config'], force=True)
    except Exception as e:
        logger.error(f"Error while replSetReconfig: {e}")
        exit(1)

    # print("ReplSetReconfig result:", result)


def shutdown_mongodb(servers, max_disk_used_server):
    try:
        service_name = servers[max_disk_used_server]['service_name']
        execute_shell_command(['systemctl', 'stop', f'{service_name}.service'], max_disk_used_server)
    except Exception as e:
        logger.error(f'error while stopping service {service_name}, {e}')
        # print(f"Error: {e}")
    logger.info(f'Stopped MongoDB on server {max_disk_used_server}, service: {service_name}.')


def start_mongodb(servers, max_disk_used_server):
    try:
        service_name = servers[max_disk_used_server]['service_name']
        execute_shell_command(['systemctl', 'start', f'{service_name}.service'], max_disk_used_server)
    except Exception as e:
        logger.error(f'error while starting service {service_name}, {e}')
        # print(f"Error: {e}")
    logger.info(f'Started MongoDB on server {max_disk_used_server}, service: {service_name}.')


def resize_oplog(client, max_disk_used_server, oplog_size=400000.0):
    try:
        client = connect(username, password, max_disk_used_server, port, False)
        # client = MongoClient(f'mongodb://root:jazzychess342@{max_disk_used_server}')
        admin_db = client.admin

        result = admin_db.command({"replSetResizeOplog": 1, "size": oplog_size})
        logger.info(f'Resized oplog size to {oplog_size}MB.')
        # print("Oplog resize result:", result)

        client.close()
    except Exception as e:
        logger.error(f"Error while resizing oplog from {max_disk_used_server}: {e}")
        exit(1)
    

def set_sync_from(servers, max_disk_used_server):# args: 
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
                        if server != max_disk_used_server and (info['syncSourceHost'] == primary_server or info['syncSourceHost'] == max_disk_used_server):
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


def delete_directory(servers, max_disk_used_server): # args : server_ip/id
    # also verify it
    try:
        res = execute_shell_command(['sudo', 'rm', '-rf', f'{servers[max_disk_used_server]["storage_path"]}'], max_disk_used_server)
        res = execute_shell_command(['sudo', 'mkdir', f'{servers[max_disk_used_server]["storage_path"]}'], max_disk_used_server)
        res = execute_shell_command(['sudo', 'chown', '-R', 'mongod:mongod', f'{servers[max_disk_used_server]["storage_path"]}'], max_disk_used_server)
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


def preprocessing(folder_path=f"{config_data['log_path']}/logs"):
    try:
        current_date = datetime.now()

        files = os.listdir(folder_path)

        valid_files = [file for file in files if '-' in file]

        if not valid_files:
            return ""

        latest_date = datetime.min
        latest_file = ""

        for file in valid_files:
            parts = file.split('__')
            file_date_str = parts[0]
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d %H:%M:%S.%f")

            if file_date > latest_date:
                latest_date = file_date
                latest_file = file

            # Check if the file is one year old from the current date
            try:
                if current_date - file_date >= timedelta(days=365):
                    file_path = os.path.join(folder_path, file)
                    os.remove(file_path)
                    logger.info(f'Deleted old file: {file}.')
                    # print("Deleted old file:", file)
            except Exception as e:
                logger.info(f'Error while deleting log file: {file}')

        server_name = latest_file.split('__')[1]

        make_high_config_server_as_primary(server_name)
    except Exception as e:
        logger.error(f"Error while preprocessing: {e}")
        exit(1)

    return server_name