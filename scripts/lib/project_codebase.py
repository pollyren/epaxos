import os

from lib.experiment_codebase import ExperimentCodebase
from utils.remote_util import is_using_tcsh, tcsh_redirect_output_to_files


class ProjectCodebase(ExperimentCodebase):

    def get_replication_protocol_arg_from_name(self, replication_protocol):
        return {
            'epaxos': 'e',
            'multi_paxos': 'mp'
        }[replication_protocol]

    def get_client_cmd(self, config, i, k, run, local_exp_directory,
                       remote_exp_directory):
        
        client = config["clients"][i]

        exp_directory = remote_exp_directory
        path_to_client_bin = os.path.join(config['base_remote_bin_directory_nfs'],
                                            config['bin_directory_name'],
                                            config['client_bin_name'])

        stats_file = os.path.join(exp_directory,
                                    config['out_directory_name'],
                                    '%s-%d-stats-%d.json' % (client, k, run))

        replication_protocol = self.get_replication_protocol_arg_from_name(config['replication_protocol'])

        expLength = config["client_experiment_length"]
        numKeys = config["client_num_keys"]
        zipfS = config["client_zipfian_s"]

        server = ""
        for region in config["server_regions"].values():
            if client in region:
                server = region[0]

        client_command = ' '.join([str(x) for x in [
            path_to_client_bin,
            replication_protocol,
            '--expLength=%d' % expLength,
            '--numKeys=%d' % numKeys,
            '--zipfS=%d' % zipfS,
            '--server=' + server
        ]])

        stdout_file = os.path.join(exp_directory,
                                    config['out_directory_name'],
                                    '%s-%d-stdout-%d.log' % (client, k, run))
        stderr_file = os.path.join(exp_directory,
                                    config['out_directory_name'],
                                    '%s-%d-stderr-%d.log' % (client, k, run))
        if is_using_tcsh(config):
            client_command = tcsh_redirect_output_to_files(client_command,
                                                            stdout_file, stderr_file)
        else:
            client_command = '%s 1> %s 2> %s' % (client_command, stdout_file,
                                                    stderr_file)

        client_command = '(cd %s; %s) & ' % (exp_directory, client_command)
        return client_command

    def get_replica_cmd(self, config, shard_idx, i, run, local_exp_directory,
                        remote_exp_directory):
        
        path_to_server_bin = os.path.join(
            config['base_remote_bin_directory_nfs'],
            config['bin_directory_name'], config['server_bin_name'])
        exp_directory = remote_exp_directory
        replica_port = config['server_port']
        replication_protocol = self.get_replication_protocol_arg_from_name(config['replication_protocol'])
        peers = [
            f"{name}:{config['server_port']}"
            for j, name in enumerate(config["server_names"])
            if j != i
        ]
        peersName2Addr = [
            f"S{j}==={name}:{config['server_port']}"
            for j, name in enumerate(config["server_names"])
            if j != i
        ]
        stats_file = os.path.join(exp_directory,
                                    config['out_directory_name'],
                                    'server-%d-stats-%d.json' % (i, run))

        replica_command = ' '.join([str(x) for x in [
            path_to_server_bin,
            replication_protocol,
            '--name=S%d' % i,
            '--port=%d' % replica_port,
            '--peers=%s' % ",".join(peers),
            '--peersName2Addr=%s' % ",".join(peersName2Addr)]])

        if replication_protocol == 'mp' and i == 0:
            replica_command += ' --is_leader=true'

        stdout_file = os.path.join(exp_directory,
                                    config['out_directory_name'], 'server-%d-stdout-%d.log' % (
                                        i, run))
        stderr_file = os.path.join(exp_directory,
                                    config['out_directory_name'], 'server-%d-stderr-%d.log' % (
                                        i, run))
        if 'default_remote_shell' in config and config['default_remote_shell'] == 'bash':
            replica_command = '%s 1> %s 2> %s' % (replica_command, stdout_file,
                                                    stderr_file)
        else:
            replica_command = tcsh_redirect_output_to_files(replica_command,
                                                            stdout_file, stderr_file)

        replica_command = 'cd %s; %s' % (exp_directory, replica_command)
        return replica_command

    def prepare_local_exp_directory(self, config, config_file):
        return super().prepare_local_exp_directory(config, config_file)

    def prepare_remote_server_codebase(self, config, server_host, local_exp_directory, remote_out_directory):
        pass

    def setup_nodes(self, config):
        pass
