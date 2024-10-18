from runner import run_xdbserver_and_xdbclient
from config import loader
import os
from test_envs import test_envs


def extract_unique_envs(test_envs):
    unique_envs = {}

    for env in test_envs:

        env_data = env['env']
        unique_env_key = (env_data['server_cpu'], env_data['client_cpu'], env_data['network'])

        src_target_pair = (env_data['src'], env_data['target'])

        if unique_env_key not in unique_envs:
            unique_envs[unique_env_key] = set()  # Use a set to avoid duplicate src/target pairs

        unique_envs[unique_env_key].add(src_target_pair)

    unique_envs = {env: list(pairs) for env, pairs in unique_envs.items()}
    return unique_envs


perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))
mode = 2
sleep = 2

envs = extract_unique_envs(test_envs)

best_config = loader.default_config

for full_env in test_envs:
    print(full_env['name'])

    if full_env['name'] == 'backup':
        for compression in ['nocomp', 'zstd', 'snappy', 'lz4', 'lzo']:
            best_config['compression_lib'] = compression
            if compression != 'nocomp' or full_env['env']['src_format'] == 2 or full_env['env']['target_format'] == 2:
                best_config['format'] = 2

            if full_env['env']['table'] == 'iotm':
                best_config['buffer_size'] = 256
                best_config['server_buffpool_size'] = best_config['deser_par'] * 30000
                best_config['client_buffpool_size'] = best_config['buffer_size'] * 3 * 10
            if full_env['env']['target'] == 'postgres':
                best_config['server_buffpool_size'] = 120000
                best_config['format'] = 1
            print("----------------------------------------")
            print("Run on env:")
            print(full_env['env'])
            print("With config:")
            print(best_config)

            t = run_xdbserver_and_xdbclient(best_config, full_env['env'], mode, perf_dir, sleep,
                                            show_output=(False, False))

            print(f"Run took: {t}s")
