import math
from itertools import product
from config.helpers import Helpers
import math


class HeuristicsOptimizer:
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
            out_throughputs[component] = self.effective_service_rate(base_throughputs[component],
                                                                     config[f"{self.ct_map_inverted[component]}_par"])

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

    def find_best_config(self, throughput_data, compression='nocomp', start_config=None):

        print("----------------------------------------")
        throughput_data = {self.ct_map_inverted[key]: value for key, value in throughput_data.items() if
                           key in self.ct_map_inverted}

        calc_throughputs = throughput_data.copy()

        best_config = {
            'read': 1,
            'deser': 1,
            'comp': 1,
            'send': 1,
            'rcv': 1,
            'decomp': 1,
            'write': 1,
        }
        workers_server = 4
        workers_client = 3

        if start_config is not None:
            best_config = {key[:-4] if key.endswith('_par') else key: value for key, value in start_config.items()}
            workers_server = sum(best_config[stage] for stage in self.server_stages)
            workers_client = sum(best_config[stage] for stage in self.client_stages)

        min_upper_bound_pair = min(self.params['upper_bounds'].items(), key=lambda x: x[1])
        lowest_upper_bound_component = min_upper_bound_pair[0]
        lowest_upper_bound_thr = min_upper_bound_pair[1]

        if compression != 'nocomp' and lowest_upper_bound_component in ['send', 'receive']:
            print(f"Adjusting upper bound from {lowest_upper_bound_thr}")
            lowest_upper_bound_thr = min_upper_bound_pair[1] / self.params[f'{compression}_ratio']
            print(f"to {lowest_upper_bound_thr} for {compression}")

        print(f"Optimizer: Minimum upper bound: {lowest_upper_bound_component}: {lowest_upper_bound_thr}")

        sorted_data = dict(sorted(throughput_data.items(), key=lambda item: item[1]))

        slowest = self.nth_slowest(calc_throughputs, 0)
        slowest_component = slowest[0]
        slowest_thr = slowest[1]

        while slowest_thr < lowest_upper_bound_thr and (
                workers_client < self.params["max_total_workers_client"] and
                workers_server < self.params["max_total_workers_server"]):
            slowest = self.nth_slowest(calc_throughputs, 0)
            slowest_component = slowest[0]
            slowest_thr = slowest[1]
            # print(f"slowest: {slowest}")
            second_slowest_thr = self.nth_slowest(calc_throughputs, 1)[1]

            while (workers_client < self.params["max_total_workers_client"] and
                   workers_server < self.params["max_total_workers_server"] and
                   slowest_thr < lowest_upper_bound_thr):

                best_config[slowest_component] = best_config[slowest_component] + 1
                new_throughput = self.effective_service_rate(throughput_data[slowest_component],
                                                             best_config[slowest_component])

                calc_throughputs[slowest_component] = new_throughput
                # print(f"{slowest_component} gets {best_config[slowest_component]} and reaches {new_throughput}")
                if slowest_component in self.server_stages:
                    workers_server += 1
                if slowest_component in self.client_stages:
                    workers_client += 1

                # print(f"workers server: {workers_server}/{max_total_workers_server}")
                # print(best_config)
                if new_throughput >= second_slowest_thr or new_throughput >= lowest_upper_bound_thr:
                    # print(f"{new_throughput} >= {second_slowest_thr} or {new_throughput} >= {lowest_upper_bound_thr}:")
                    break

        highest_net = max(best_config['send'], best_config['rcv'])
        # TODO: current setup allows up to 4
        highest_net = min(highest_net, 2)
        best_config['send'] = best_config['rcv'] = highest_net

        total_server_workers = sum(best_config[stage] for stage in self.server_stages)
        total_client_workers = sum(best_config[stage] for stage in self.client_stages)
        best_config['buffer_size'] = 256  # Helpers.get_cache_size_in_kib(1)
        best_config['server_buffpool_size'] = best_config['buffer_size'] * total_server_workers * 20
        best_config['client_buffpool_size'] = best_config['buffer_size'] * total_client_workers * 20

        print("Optimizer: Initial base throughputs")
        print(throughput_data)
        print("Heuristic Optimizer: Decided config")
        print(self.format_config(best_config))
        print("Heuristic Optimizer: Estimated throughputs")
        print(calc_throughputs)
        print("----------------------------------------")

        return self.format_config(best_config)

    '''
    def opt_with_comp(self, best_config, throughput_data):
        calc_throughputs = self.calculate_throughput(best_config, throughput_data, True)
        # print(f"min after opt: {calc_throughputs}")
        slowest_comp = self.nth_slowest(calc_throughputs, 0)
        slowest = next(iter(slowest_comp))
        if slowest in ['send', 'rcv']:
            print("Trying to compress")
            throughput_data['send'] = throughput_data['send'] * (1 / self.params['snappy_ratio'])
            throughput_data['rcv'] = throughput_data['rcv'] * (1 / self.params['snappy_ratio'])
            throughput_data['comp'] = self.params['comp_snappy']
            throughput_data['decomp'] = self.params['decomp_snappy']

            self.params['upper_bounds']['send'] *= (1 / self.params['snappy_ratio'])
            self.params['upper_bounds']['rcv'] *= (1 / self.params['snappy_ratio'])
            # print(throughput_data)
            new_best_config = self.find_best_config(throughput_data)
            new_best_config['compression_lib'] = 'snappy'

            new_throughputs = self.calculate_throughput(new_best_config, throughput_data, True)
            new_slowest = self.nth_slowest(new_throughputs, 0)[1]
            print(f"new slowest {new_slowest} vs slowest {slowest_comp[1]}")
            if new_slowest > slowest_comp[1]:
                print("Compression pays off")
                return new_best_config
            else:
                print("Compression does not pay off")
                return best_config
            # print(new_best_config)
            # print("new throughputs")
            # print(self.calculate_throughput(new_best_config, throughput_data, True))
    '''
