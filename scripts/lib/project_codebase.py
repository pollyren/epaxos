import os

from lib.experiment_codebase import ExperimentCodebase
from utils.remote_util import tcsh_redirect_output_to_files


class ProjectCodebase(ExperimentCodebase):

    def get_replication_protocol_arg_from_name(self, replication_protocol):
        return {
            'epaxos': 'e',
            'multi_paxos': 'mp'
        }[replication_protocol]

    def get_client_cmd(self, config, i, j, k, run, local_exp_directory,
                       remote_exp_directory):
        if 'run_locally' in config and config['run_locally']:
            exp_directory = local_exp_directory
            path_to_client_bin = os.path.join(config['src_directory'],
                                              config['bin_directory_name'], config['client_bin_name'])
            master_addr = 'localhost'
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'], 'client-%d-%d' % (
                                          i, j),
                                      'client-%d-%d-%d-stats-%d.json' % (i, j, k, run))
        else:
            exp_directory = remote_exp_directory
            path_to_client_bin = os.path.join(
                config['base_remote_bin_directory_nfs'],
                config['bin_directory_name'], config['client_bin_name'])
            master_addr = config['master_server_name']
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'],
                                      'client-%d-%d-%d-stats-%d.json' % (i, j, k, run))

        client_id = i * config['client_nodes_per_server'] * \
            config['client_processes_per_client_node'] + j * \
            config['client_processes_per_client_node'] + k
        client_command = ' '.join([str(x) for x in [
            path_to_client_bin,
            '-clientId', client_id,
            '-expLength', config['client_experiment_length'],
            '-masterAddr', master_addr,
            '-masterPort', config['master_port'],
            '-maxProcessors', config['client_max_processors'],
            '-numKeys', config['client_num_keys'],
            '-rampDown', config['client_ramp_down'],
            '-rampUp', config['client_ramp_up'],
            '-reads', config['client_read_percentage'],
            '-replProtocol', config['replication_protocol'],
            '-rmws', config['client_rmw_percentage'],
            '-statsFile', stats_file,
            '-writes', config['client_write_percentage'],
        ]])
        if 'client_cpuprofile' in config and config['client_cpuprofile']:
            client_command += ' -cpuProfile %s' % os.path.join(exp_directory,
                                                               config['out_directory_name'],
                                                               'client-%d-%d-%d-cpuprof-%d.log' % (i, j, k, run))
        if 'client_rand_sleep' in config:
            client_command += ' -randSleep %d' % config['client_rand_sleep']
        if config['client_conflict_percentage'] < 0:
            client_command += ' -zipfS %f' % config['client_zipfian_s']
            client_command += ' -zipfV %f', config['client_zipfian_v'],
        else:
            client_command += ' -conflicts %s' % config['client_conflict_percentage']
        # TODO need logic for how to determine leader for various protocols
        if config['replication_protocol'] == 'gpaxos':
            client_command += ' -fastPaxos'
        if config['client_random_coordinator']:
            client_command += ' -randomLeader'
        if config['client_debug_output']:
            client_command += ' -debug'
        if 'server_epaxos_mode' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_epaxos_mode']:
            client_command += ' -epaxosMode'

        if 'server_emulate_wan' in config and not config['server_emulate_wan']:
            client_command += ' -defaultReplicaOrder'
            client_command += ' -forceLeader %d' % i

        if config['replication_protocol'] == 'abd':
            if config['replication_protocol_settings']['client_regular_consistency']:
                client_command += ' -regular'
        elif config['replication_protocol'] == 'gryff':
            if config['replication_protocol_settings']['client_regular_consistency']:
                client_command += ' -regular'
            if config['replication_protocol_settings']['client_sequential_consistency']:
                client_command += ' -sequential'
        if 'proxy_operations' in config['replication_protocol_settings'] and config['replication_protocol_settings']['proxy_operations']:
            client_command += ' -proxy'
        if 'server_thrifty' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_thrifty']:
            client_command += ' -thrifty'
        if 'client_tail_at_scale' in config and config['client_tail_at_scale'] > 0:
            client_command += ' -tailAtScale %d' % config['client_tail_at_scale']
        if 'client_gc_debug_trace' in config and config['client_gc_debug_trace']:
            if 'run_locally' in config and config['run_locally']:
                client_command = 'GODEBUG=\'gctrace=1\'; %s' % client_command
            else:
                client_command = 'setenv GODEBUG gctrace=1; %s' % client_command
        if 'client_disable_gc' in config and config['client_disable_gc']:
            if 'run_locally' in config and config['run_locally']:
                client_command = 'GOGC=off; %s' % client_command
            else:
                client_command = 'setenv GOGC off; %s' % client_command

        if 'run_locally' in config and config['run_locally']:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'client-%d-%d' % (i, j),
                                       'client-%d-%d-%d-stdout-%d.log' % (i, j, k, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'client-%d-%d' % (i, j),
                                       'client-%d-%d-%d-stderr-%d.log' % (i, j, k, run))
            client_command = '%s 1> %s 2> %s' % (client_command, stdout_file,
                                                 stderr_file)
        else:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'client-%d-%d-%d-stdout-%d.log' % (i, j, k, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'client-%d-%d-%d-stderr-%d.log' % (i, j, k, run))
            if 'default_remote_shell' in config and config['default_remote_shell'] == 'bash':
                client_command = '%s 1> %s 2> %s' % (client_command, stdout_file,
                                                     stderr_file)
            else:
                client_command = tcsh_redirect_output_to_files(client_command,
                                                               stdout_file, stderr_file)

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
            '-name', 'S%d' % i,
            '-port', replica_port,
            '-peers', ",".join(peers),
            '-peersName2Addr', ",".join(peersName2Addr)]])

        if replication_protocol == 'mp' and i == 0:
            replica_command += ' -is_leader=true'

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
