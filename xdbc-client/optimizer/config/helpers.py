import pandas as pd
import numpy as np


class Helpers:
    @staticmethod
    def get_cratios(libs, buffer_size, table, perf_dir):

        general_stats = pd.read_csv(f"{perf_dir}/xdbc_general_stats.csv")

        cratio_dict = {
            "lzo_ratio": 0,
            "snappy_ratio": 0,
            "lz4_ratio": 0,
            "zstd_ratio": 0,
            "nocomp_ratio": 1
        }

        nocomp_avg = filtered_stats[filtered_stats['compression'] == 'nocomp']['datasize'].mean()

        for lib in libs:
            if lib != 'nocomp':
                avg_datasize = filtered_stats[filtered_stats['compression'] == lib]['datasize'].mean()

                if nocomp_avg > 0:  # Avoid division by zero
                    cratio_dict[f"{lib}_ratio"] = avg_datasize / nocomp_avg
                else:
                    cratio_dict[f"{lib}_ratio"] = 0  # Handle cases where nocomp_avg is zero
        # print(cratio_dict)
        return cratio_dict

    @staticmethod
    def get_cache_size_in_kib(level):
        try:
            with open(f"/sys/devices/system/cpu/cpu0/cache/index{level}/size", 'r') as f:
                size_str = f.read().strip()
                if size_str.endswith('K'):
                    return int(size_str[:-1])  # Already in KiB
                elif size_str.endswith('M'):
                    return int(size_str[:-1]) * 1024  # Convert MiB to KiB
        except FileNotFoundError:
            return None

    @staticmethod
    def get_best_comp_config(compconfigs):
        print(compconfigs)
        best = next(iter(compconfigs))
        for comp in compconfigs.keys():
            if compconfigs[comp]['thr'] > compconfigs[best]['thr']:
                best = comp

        return best

    @staticmethod
    def compute_serial_fractions(environment, perf_dir, base_rates):

        try:
            general_stats = pd.read_csv(f"{perf_dir}/xdbc_general_stats.csv")
            server_timings = pd.read_csv(f"{perf_dir}/xdbc_server_timings.csv")
            client_timings = pd.read_csv(f"{perf_dir}/xdbc_client_timings.csv")
        except Exception as e:
            print(f"Error loading files: {e}")
            return None

        filtered_stats = general_stats[
            (general_stats['server_cpu'] == environment['server_cpu']) &
            (general_stats['client_cpu'] == environment['client_cpu']) &
            (general_stats['network'] == environment['network']) &
            (general_stats['source_system'] == environment['src']) &
            (general_stats['target_system'] == environment['target'])
            ]

        if filtered_stats.empty:
            return None

        joined_server = pd.merge(filtered_stats, server_timings, left_on='date', right_on='transfer_id', how='inner')
        joined_client = pd.merge(filtered_stats, client_timings, left_on='date', right_on='transfer_id', how='inner')
        # print(joined_server.columns)

        server_comps = ['read', 'deser', 'comp', 'send']
        client_comps = ['rcv', 'decomp', 'write']

        data = []

        for comp in server_comps + client_comps:

            df = joined_client
            if comp in server_comps:
                df = joined_server
            parallelism_values = df[f'{comp}_par']
            throughput_values = df[f'{comp}_throughput_pb']

            component_data = pd.DataFrame({
                'component': comp,
                'parallelism': parallelism_values,
                'throughput': throughput_values
            })

            data.append(component_data)

        result_df = pd.concat(data, ignore_index=True)
        grouped_df = result_df.groupby(['component', 'parallelism'])['throughput'].mean().reset_index()
        grouped_df['overall_throughput'] = grouped_df['parallelism'] * grouped_df['throughput']

        base_throughput = grouped_df[grouped_df['parallelism'] == 1].set_index('component')['throughput']

        def calculate_serial_fraction(row):

            mu_1_c = base_throughput[row['component']]

            mu_w_c = row['overall_throughput']
            w = row['parallelism']

            if w == 1:
                return 0

            s = (mu_1_c * w - mu_w_c) / (mu_1_c * (w - 1))
            return max(0, min(s, 1))  # Clamp s between 0 and 1

        grouped_df['s'] = grouped_df.apply(calculate_serial_fraction, axis=1)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        print(grouped_df)
        return {}

    @staticmethod
    def load_throughput(environment, perf_dir, compression='nocomp'):

        try:
            general_stats = pd.read_csv(f"{perf_dir}/xdbc_general_stats.csv")
            server_timings = pd.read_csv(f"{perf_dir}/xdbc_server_timings.csv")
            client_timings = pd.read_csv(f"{perf_dir}/xdbc_client_timings.csv")
        except Exception as e:
            print(f"Error loading files: {e}")  # Optional: Print the error for debugging
            return None

        filtered_stats = general_stats[
            (general_stats['server_cpu'] == environment['server_cpu']) &
            (general_stats['client_cpu'] == environment['client_cpu']) &
            (general_stats['network'] == environment['network']) &
            (general_stats['source_system'] == environment['src']) &
            (general_stats['target_system'] == environment['target']) &
            (general_stats['table'] == environment['table']) &
            (general_stats['compression'] == compression)
            ]

        par_columns = filtered_stats.filter(like='_par').columns
        filtered_stats = filtered_stats[(filtered_stats[par_columns] == 1).all(axis=1)]

        if filtered_stats.empty:
            return None

        # print(filtered_stats)

        transfer_dates = filtered_stats['date'].unique()
        # print(transfer_dates)

        matching_server_timings = server_timings[server_timings['transfer_id'].isin(transfer_dates)]
        matching_client_timings = client_timings[client_timings['transfer_id'].isin(transfer_dates)]

        server_throughputs = matching_server_timings.filter(like='throughput_pb')
        client_throughputs = matching_client_timings.filter(like='throughput_pb')

        server_throughputs = server_throughputs.add_prefix('server_')
        client_throughputs = client_throughputs.add_prefix('client_')

        combined_throughputs = pd.concat([server_throughputs.reset_index(drop=True),
                                          client_throughputs.reset_index(drop=True)], axis=1)

        averaged_throughputs = combined_throughputs.mean().to_dict()

        # simplification for measuring the network throughput
        net_throughput = max(averaged_throughputs["server_send_throughput_pb"],
                             averaged_throughputs["client_rcv_throughput_pb"])
        averaged_throughputs["server_send_throughput_pb"] = averaged_throughputs[
            "client_rcv_throughput_pb"] = net_throughput
        return averaged_throughputs

    @staticmethod
    def read_last_line(file_path):
        with open(file_path, 'r') as file:
            lines = file.readlines()
            last_line = lines[-1].strip()
            return last_line

    @staticmethod
    def calculate_average_throughputs(df_merged, throughput_data):

        components = [col.replace('_throughput_pb', '') for col in df_merged.columns if col.endswith('_throughput_pb')]

        throughput_sums = {component: 0 for component in components}
        throughput_counts = {component: 0 for component in components}

        for component in components:
            throughput_pb_col = f'{component}_throughput_pb'

            if component in throughput_data:
                throughput_per_buffer = df_merged[throughput_pb_col] * df_merged[f"{component}_par"]

                throughput_sums[component] += throughput_per_buffer.sum()
                throughput_counts[component] += len(throughput_per_buffer)

        averaged_throughputs = {component: throughput_sums[component] / throughput_counts[component]
                                for component in components if throughput_counts[component] > 0}

        # Merge with the provided throughput_data
        # for component in throughput_data:
        #    if component in averaged_throughputs:
        #        averaged_throughputs[component] = (averaged_throughputs[component] + throughput_data[component]) / 2
        #    else:
        #        averaged_throughputs[component] = throughput_data[component]

        return averaged_throughputs
