from .helpers import Helpers


class MetricsClient:
    def __init__(self, transfer_id, total_time, rcv_wait_time, rcv_proc_time, rcv_throughput, rcv_throughput_pb,
                 rcv_load,
                 decomp_wait_time, decomp_proc_time, decomp_throughput, decomp_throughput_pb, decomp_load,
                 write_wait_time, write_proc_time, write_throughput, write_throughput_pb, write_load):
        self.transfer_id = transfer_id
        self.total_time = total_time
        self.rcv_wait_time = rcv_wait_time
        self.rcv_proc_time = rcv_proc_time
        self.rcv_throughput = rcv_throughput
        self.rcv_throughput_pb = rcv_throughput_pb
        self.rcv_load = rcv_load
        self.decomp_wait_time = decomp_wait_time
        self.decomp_proc_time = decomp_proc_time
        self.decomp_throughput = decomp_throughput
        self.decomp_throughput_pb = decomp_throughput_pb
        self.decomp_load = decomp_load
        self.write_wait_time = write_wait_time
        self.write_proc_time = write_proc_time
        self.write_throughput = write_throughput
        self.write_throughput_pb = write_throughput_pb
        self.write_load = write_load

    @staticmethod
    def from_csv(file_path):
        last_line = Helpers.read_last_line(file_path)
        values = last_line.split(',')

        if len(values) != 17:
            raise ValueError("The number of values in the last line does not match the number of class attributes.")

        return MetricsClient(
            transfer_id=int(values[0]),
            total_time=float(values[1]),
            rcv_wait_time=float(values[2]),
            rcv_proc_time=float(values[3]),
            rcv_throughput=float(values[4]),
            rcv_throughput_pb=float(values[5]),
            rcv_load=float(values[6]),
            decomp_wait_time=float(values[7]),
            decomp_proc_time=float(values[8]),
            decomp_throughput=float(values[9]),
            decomp_throughput_pb=float(values[10]),
            decomp_load=float(values[11]),
            write_wait_time=float(values[12]),
            write_proc_time=float(values[13]),
            write_throughput=float(values[14]),
            write_throughput_pb=float(values[15]),
            write_load=float(values[16])
        )

    def to_dict(self):
        return {
            "transfer_id": self.transfer_id,
            "total_time": self.total_time,
            "rcv_wait_time": self.rcv_wait_time,
            "rcv_proc_time": self.rcv_proc_time,
            "rcv_throughput": self.rcv_throughput,
            "rcv_throughput_pb": self.rcv_throughput_pb,
            "rcv_load": self.rcv_load,
            "decomp_wait_time": self.decomp_wait_time,
            "decomp_proc_time": self.decomp_proc_time,
            "decomp_throughput": self.decomp_throughput,
            "decomp_throughput_pb": self.decomp_throughput_pb,
            "decomp_load": self.decomp_load,
            "write_wait_time": self.write_wait_time,
            "write_proc_time": self.write_proc_time,
            "write_throughput": self.write_throughput,
            "write_throughput_pb": self.write_throughput_pb,
            "write_load": self.write_load
        }

    def get_throughput_metrics(self, toStr=True):
        dict = {
            "rcv_throughput": self.rcv_throughput,
            "rcv_throughput_pb": self.rcv_throughput_pb,
            "decomp_throughput": self.decomp_throughput,
            "decomp_throughput_pb": self.decomp_throughput_pb,
            "write_throughput": self.write_throughput,
            "write_throughput_pb": self.write_throughput_pb
        }
        if not toStr:
            return dict
        return str(dict)

    def __str__(self):
        return str(self.to_dict())
