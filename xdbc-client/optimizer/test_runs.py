import os
import csv
from optimize import optimize
from test_envs import test_envs, expert_configs

file_exists = os.path.isfile('local_measurements/test_runs.csv')
optimizer_results = []
best_configs = []
with open('local_measurements/test_runs.csv', mode='a', newline='') as csv_file:
    fieldnames = ['env_name', 'optimizer', 'runtime', 'opt_time', 'est_throughput'] + list(
        test_envs[0]['env'].keys()) + list(expert_configs[0]['config'].keys())
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    if not file_exists:
        writer.writeheader()

    for env in test_envs:
        env_name = env['name']

        if True:  # env_name == 'etl':
            # Run default optimizer (xdbc)
            for xdbc_optimizer in ['heuristic']:
                runtime, best_config, est_thr, opt_time = optimize(env_name, 'xdbc', xdbc_optimizer)
                row = {'env_name': env_name, 'optimizer': f'xdbc-{xdbc_optimizer}', 'runtime': runtime,
                       'opt_time': opt_time, 'est_throughput': est_thr}
                optimizer_results += [(env_name, f'xdbc-{xdbc_optimizer}', est_thr)]
                best_configs += [(env_name, best_config)]
                print(row)
                row.update(env['env'])
                row.update(best_config)
                row = {key: value for key, value in row.items() if key in fieldnames}

                writer.writerow(row)  # Write to CSV
                csv_file.flush()

            # Run expert optimizer for matching expert config
            for expert_config in expert_configs:
                if True:  # expert_config['name'] == 'etl':
                    runtime, best_config, est_thr, opt_time = optimize(env_name, 'expert', expert_config['name'])
                    row = {'env_name': env_name, 'optimizer': f'expert_{expert_config["name"]}', 'runtime': runtime,
                           'opt_time': opt_time, 'est_throughput': est_thr}
                    print(row)
                    optimizer_results += [(env_name, f'expert_{expert_config["name"]}', est_thr)]
                    row.update(env['env'])
                    row.update(best_config)
                    row = {key: value for key, value in row.items() if key in fieldnames}
                    writer.writerow(row)
                    csv_file.flush()

print(optimizer_results)
print(best_configs)
