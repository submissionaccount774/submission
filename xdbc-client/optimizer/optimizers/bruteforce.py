import math
from itertools import product
from config.helpers import Helpers
import math
import datetime
import os
from test_envs import test_envs


class BruteforceOptimizer:
    def __init__(self, params):
        self.params = params
        self.ct_map_inverted = {
            'server_read_throughput_pb': 'read',
            'server_deser_throughput_pb': 'deser',
            'server_comp_throughput_pb': 'comp',
            'server_send_throughput_pb': 'send',
            'client_rcv_throughput_pb': 'rcv',
            'client_decomp_throughput_pb': 'decomp',
            'client_write_throughput_pb': 'write'
        }
        self.server_stages = ['read', 'deser', 'comp', 'send']
        self.client_stages = ['rcv', 'decomp', 'write']

    def effective_service_rate(self, base_rate, workers):
        f0 = self.params["f0"]
        a = self.params["a"]
        return (f0 + (1 - f0) * workers) * base_rate

    def calculate_throughput(self, config, base_throughputs, return_throughputs=False):

        # print("incoming throughput data")
        # print(base_throughputs)
        out_throughputs = {}

        for component in base_throughputs.keys():
            # print(f'{component} : {calc_thr[component]}')
            # print("config")
            # print(config)
            out_throughputs[component] = self.effective_service_rate(base_throughputs[component],
                                                                     config[f"{self.ct_map_inverted[component]}_par"])

            if component in ('rcv_par', 'send_par'):
                out_throughputs[component] = out_throughputs[component] / self.params[
                    f"{config['compression_lib']}_ratio"]

        # print("updated throughputs")
        # print(out_throughputs)
        if return_throughputs:
            return out_throughputs
        return self.nth_slowest(out_throughputs, 0)[1]

    def nth_slowest(self, data, n):
        # Sort the dictionary by value in descending order
        sorted_items = sorted(data.items(), key=lambda item: item[1])

        # Return the n-th item from the sorted list
        return sorted_items[n] if n < len(sorted_items) else None

    def format_config(self, config):
        # Create a new dictionary with updated keys
        formatted_config = {
            (f"{key}_par" if key in self.server_stages or key in self.client_stages else key): value
            for key, value in config.items()
        }

        return formatted_config

    def find_best_config(self, throughput_data, compression='nocomp'):

        print("----------------------------------------")
        print("Started Bruteforce Optimizer")
        # print(throughput_data)

        calc_throughputs = throughput_data.copy()

        max_server = self.params["max_total_workers_server"]
        max_client = self.params["max_total_workers_client"]
        complibs = ['nocomp', 'zstd', 'lz4', 'lzo', 'snappy']
        perf_dir = os.path.abspath(os.path.join(os.getcwd(), 'local_measurements'))

        throughput_data_comps = {}
        for complib in complibs:
            throughput_data_comps[complib] = Helpers.load_throughput(self.params["env"], perf_dir, compression=complib)

        min_upper_bound_pair = min(self.params['upper_bounds'].items(), key=lambda x: x[1])
        lowest_upper_bound_component = min_upper_bound_pair[0]
        lowest_upper_bound_thr = min_upper_bound_pair[1]

        config = {}
        config['read_par'] = 1
        config['deser_par'] = 1
        config['comp_par'] = 1
        config['send_par'] = 1
        config['rcv_par'] = 1
        config['decomp_par'] = 1
        config['write_par'] = 1
        config['compression_lib'] = 'nocomp'
        # print(f"LOWEST UPPER BOUND {lowest_upper_bound_component} with {lowest_upper_bound_thr}")
        max_throughput = 0
        best_config = {}
        config = {}
        pruned = 0
        evaluated = 0

        for comp in complibs:
            lowest = lowest_upper_bound_thr
            if lowest_upper_bound_component in ['send', 'rcv'] and comp != 'nocomp':
                lowest = lowest_upper_bound_thr * 1 / self.params[
                    f"{comp}_ratio"]
            for read_par in range(1, max_server + 1):
                for deser_par in range(1, max_server + 1 - read_par):
                    for comp_par in range(1, max_server + 1 - (read_par + deser_par)):
                        for send_par in range(1, max_server + 1 - (read_par + deser_par + comp_par)):
                            for rcv_par in range(1, max_client + 1):
                                if send_par != rcv_par:
                                    break
                                for decomp_par in range(1, max_client + 1 - rcv_par):
                                    for write_par in range(1, max_client + 1 - (rcv_par + decomp_par)):

                                        config['read_par'] = read_par
                                        config['deser_par'] = deser_par
                                        config['comp_par'] = comp_par
                                        config['send_par'] = send_par
                                        config['rcv_par'] = rcv_par
                                        config['decomp_par'] = decomp_par
                                        config['write_par'] = write_par
                                        config['compression_lib'] = comp
                                        evaluated += 1

                                        thrpt = self.calculate_throughput(config, throughput_data_comps[comp])
                                        if thrpt <= lowest and thrpt > max_throughput:
                                            # print(f"chose {thrpt} over {max_throughput} with {config}")
                                            # print(f"over {best_config}")
                                            max_throughput = thrpt
                                            best_config = config.copy()

        total_server_workers = sum(best_config[f"{stage}_par"] for stage in self.server_stages)
        total_client_workers = sum(best_config[f"{stage}_par"] for stage in self.client_stages)
        best_config['buffer_size'] = 256  # Helpers.get_cache_size_in_kib(1)
        best_config['server_buffpool_size'] = best_config['buffer_size'] * total_server_workers * 20
        best_config['client_buffpool_size'] = best_config['buffer_size'] * total_client_workers * 20

        calc_throughputs = self.calculate_throughput(best_config, throughput_data_comps[best_config['compression_lib']],
                                                     True)
        print(f"Bruteforce optimizer finished")
        print("Bruteforce optimizer: Decided config")
        print(self.format_config(best_config))
        print("Bruteforce optimizer, Base throughputs")
        print(throughput_data)
        print("Bruteforce optimizer: Estimated throughputs")
        print(calc_throughputs)
        print(f"Bruteforce optimizer: Evaluated {evaluated}, Pruned {pruned}")
        print("----------------------------------------")

        return self.format_config(best_config)
