from lib.project_codebase import ProjectCodebase


__BUILDERS__ = {
    "project": ProjectCodebase()
}


def get_client_cmd(config, i, k, run, local_exp_directory,
                   remote_exp_directory):
    return __BUILDERS__[config['codebase_name']].get_client_cmd(config, i, k, run,
                                                                local_exp_directory,
                                                                remote_exp_directory)


def get_replica_cmd(config, shard_idx, replica_idx, run, local_exp_directory,
                    remote_exp_directory):
    return __BUILDERS__[config['codebase_name']].get_replica_cmd(config, shard_idx,
                                                                 replica_idx, run,
                                                                 local_exp_directory,
                                                                 remote_exp_directory)


def prepare_local_exp_directory(config, config_file):
    return __BUILDERS__[config['codebase_name']].prepare_local_exp_directory(config, config_file)


def prepare_remote_server_codebase(config, server_host, local_exp_directory, remote_out_directory):
    return __BUILDERS__[config['codebase_name']].prepare_remote_server_codebase(config, server_host, local_exp_directory, remote_out_directory)


def setup_nodes(config):
    return __BUILDERS__[config['codebase_name']].setup_nodes(config)
