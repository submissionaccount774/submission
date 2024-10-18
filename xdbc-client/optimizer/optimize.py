from optimizers import HeuristicsOptimizer, BruteforceOptimizer
from runner import run_xdbserver_and_xdbclient, print_metrics
from config import loader
from config.helpers import Helpers
from test_envs import test_envs, expert_configs
import os
import math
import datetime
import time


def optimize(env_name, optimizer, optimizer_opt):
    print("-------------Starting Optimization-----------------")
    print(optimizer_opt)
    env = next((entry['env'] for entry in test_envs if entry['name'] == env_name), None)
    sleep = 2
    mode = 2
    perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

    throughput_data = Helpers.load_throughput(env, perf_dir)

    optimize = False
    if throughput_data is None:
        print(f"No log information for env: {env}")
        print(f"Running the default config: {loader.default_config}")
    else:
        # s = Helpers.compute_serial_fractions(env, perf_dir, throughput_data)
        # print(s)
        print(f"Found average throughput data: ")
        # print(f"{throughput_data}")
        print("Running the optimizer")
        optimize = True
        params = {"f0": 0.3,
                  "a": 0.02,
                  "upper_bounds": loader.upper_bounds[f"{env['target']}_{env['src']}"][mode],
                  "max_total_workers_server": math.floor(env["server_cpu"] * 1.2),
                  "max_total_workers_client": math.floor(env["client_cpu"] * 1.2),
                  "compression_libraries": ["lzo", "snappy", "nocomp", "lz4", "zstd"],
                  "env": env
                  }
    if optimizer == 'xdbc':
        print("Chose XDBC optimizer")
        optimize = True
        best_config = loader.default_config

        net = 10000
        if env["network"] != 0:
            net = env["network"]
        params['upper_bounds']['send'] = net
        params['upper_bounds']['rcv'] = net

        params.update(
            Helpers.get_cratios(params['compression_libraries'], best_config['buffer_size'], env['table'], perf_dir))

        optimizer = HeuristicsOptimizer(params)
        if optimizer_opt == 'bruteforce':
            optimizer = BruteforceOptimizer(params)


    else:
        best_config = next((config['config'] for config in expert_configs if config['name'] == optimizer_opt), None)

        print(f"Chose Expert config: {best_config}")
        optimize = False

    # Generate config
    opt_time = 0
    if optimize:
        best_config['compression_lib'] = 'nocomp'
        start_opt_time = time.perf_counter()
        best_config = optimizer.find_best_config(throughput_data)
        end_opt_time = time.perf_counter()
        total_time = end_opt_time - start_opt_time
        opt_time = total_time * 1_000_000
        # print(best_config)
        # print(throughput_data)
        max_throughput = optimizer.calculate_throughput(best_config, throughput_data)

        min_upper_bound_pair = min(loader.upper_bounds[f"{env['target']}_{env['src']}"][mode].items(),
                                   key=lambda x: x[1])
        lowest_upper_bound_component = min_upper_bound_pair[0]
        # print(f"lowest upper bound: {lowest_upper_bound_component}")

        if lowest_upper_bound_component in ['send', 'rcv'] and optimizer_opt != 'heuristics':
            complibs = ['zstd', 'lz4', 'lzo', 'snappy']

            compconfigs = {}
            compconfigs['nocomp'] = {}
            compconfigs['nocomp']['thr'] = max_throughput
            compconfigs['nocomp']['config'] = best_config

            for complib in complibs:

                throughput_data_compress = Helpers.load_throughput(env, perf_dir, compression=complib)
                if throughput_data_compress is None:
                    print(f"No data for compressor {complib}")
                else:

                    compconfigs[complib] = {}
                    print(f"Found average throughput data for {complib}:")
                    # print(throughput_data_compress)
                    compconfigs[complib]['config'] = optimizer.find_best_config(throughput_data_compress,
                                                                                compression=complib,
                                                                                start_config=best_config)
                    compconfigs[complib]['thr'] = optimizer.calculate_throughput(compconfigs[complib]['config'],
                                                                                 throughput_data_compress)

            end_opt_time = time.perf_counter()
            total_time = end_opt_time - start_opt_time
            opt_time = total_time * 1_000_000
            best_comp = Helpers.get_best_comp_config(compconfigs)
            best_config = compconfigs[best_comp]['config']
            best_config['compression_lib'] = best_comp

            # best_config = optimizer.find_best_config(throughput_data)

    if 'compression_lib' not in best_config:
        best_config['compression_lib'] = 'nocomp'
    best_config['format'] = 1
    if (env['src_format'] == 2 or env['target_format'] == 2) or best_config['compression_lib'] != 'nocomp':
        best_config['format'] = 2


    # Got the config, now run

    t = -1
    t = run_xdbserver_and_xdbclient(best_config, env, mode, perf_dir, sleep, show_output=(False, False))
    print("Optimization time:", opt_time)
    print("Actual time:", t)
    # print("Estimated time:", 9200 / throughput)
    print("Actual total throughput:", 9200 / t)
    # print("Estimated total throughput:", throughput)

    if t > 0:
        print("Real throughputs:")
        real = print_metrics(perf_dir, dict=True)
        print(real)

    throughput_data_compress = Helpers.load_throughput(env, perf_dir, compression=best_config['compression_lib'])
    if optimize:
        print("Estimated throughputs:")

        estimated_thr = optimizer.calculate_throughput(best_config, throughput_data_compress, False)
        print(estimated_thr)
        estimated_detailed = optimizer.calculate_throughput(best_config, throughput_data_compress, True)
        print(estimated_detailed)
    else:
        optimizer = HeuristicsOptimizer(params)
        estimated_thr = optimizer.calculate_throughput(best_config, throughput_data_compress, False)

    return t, best_config, estimated_thr, opt_time
    '''
    # Calculate the difference
    modified_real = {
        key.replace('_throughput_pb', ''): value * best_config[key.replace('_throughput_pb', '_par')]
        for key, value in real.items()
        if key.endswith('_throughput_pb')
    }

    # Calculate the difference
    differences = {}

    for key in estimated:
        if key in modified_real:
            differences[key] = estimated[key] - modified_real[key]

    print("Prediction errors:")
    print(differences)
    '''
