import argparse
from optimize import optimize


def main():
    parser = argparse.ArgumentParser(description="Run XDBC experiments.")

    parser.add_argument('--env_name', type=str, required=True, help="The name of the environment.")
    parser.add_argument('--optimizer', type=str, required=True, help="Use xdbc or expert configs")
    parser.add_argument('--optimizer_spec', type=str, required=False, help="Provide expert config")
    args = parser.parse_args()

    optimizer_spec = args.optimizer_spec

    if args.optimizer == 'expert' and args.optimizer_spec is None:
        optimizer_spec = args.env_name
    if args.optimizer == 'xdbc' and args.optimizer is None:
        optimizer_spec = 'heuristic'

    # TODO: split optimizer and runner
    runtime, best_config, estimated_thr, opt_time = optimize(args.env_name, args.optimizer, optimizer_spec)
    print(f"took {runtime}")


if __name__ == "__main__":
    main()
