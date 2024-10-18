test_envs = [
    {
        'name': "iot_analysis",
        'env': {
            'server_cpu': 16,
            'client_cpu': 8,
            'network': 100,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'postgres',
            'src_format': 1,
            'target': 'pandas',
            'target_format': 2,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcpython',
            'table': 'iotm'
        }
    },
    {
        'name': "backup",
        'env': {
            'server_cpu': 32,
            'client_cpu': 16,
            'network': 1000,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'postgres',
            'src_format': 1,
            'target': 'csv',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'table': 'lineitem_sf10'
        }
    },
    {
        'name': "icu_analysis",
        'env': {
            'server_cpu': 16,
            'client_cpu': 12,
            'network': 50,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'pandas',
            'target_format': 2,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcpython',
            'table': 'inputeventsm'
        }
    },
    {
        'name': "copy",
        'env': {
            'server_cpu': 8,
            'client_cpu': 8,
            'network': 0,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'csv',
            'src_format': 1,
            'target': 'csv',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcclient',
            'table': 'lineitem_sf10'
        }
    },
    {
        'name': "etl",
        'env': {
            'server_cpu': 8,
            'client_cpu': 8,
            'network': 500,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'postgres',
            'src_format': 1,
            'target': 'spark',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcspark',
            'table': 'lineitem_sf10'
        }
    },
    {
        'name': "pg",
        'env': {
            'server_cpu': 16,
            'client_cpu': 16,
            'network': 100,
            'network_latency': 0,
            'network_loss': 0,
            'src': 'postgres',
            'src_format': 1,
            'target': 'postgres',
            'target_format': 1,
            'server_container': 'xdbcserver',
            'client_container': 'xdbcpostgres',
            'table': 'lineitem_sf10'
        }
    }

]

expert_configs = [
    {
        'name': "iot_analysis",
        'config': {
            'read_par': 4,
            'deser_par': 4,
            'comp_par': 4,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 3,
            'write_par': 3,
            'compression_lib': 'zstd',
            'buffer_size': 128,
            'server_buffpool_size': 4 * 128 * 20,
            'client_buffpool_size': 2 * 128 * 20,
            'format': 2
        }
    },
    {
        'name': "backup",
        'config': {
            'read_par': 8,
            'deser_par': 8,
            'comp_par': 4,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 4,
            'write_par': 10,
            'compression_lib': 'snappy',
            'buffer_size': 256,
            'server_buffpool_size': 8 * 256 * 20,
            'client_buffpool_size': 4 * 256 * 20,
            'format': 1
        }
    },
    {
        'name': "icu_analysis",
        'config': {
            'read_par': 4,
            'deser_par': 4,
            'comp_par': 2,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 2,
            'write_par': 2,
            'compression_lib': 'zstd',
            'buffer_size': 64,
            'server_buffpool_size': 4 * 64 * 20,
            'client_buffpool_size': 2 * 64 * 20,
            'format': 2
        }
    },
    {
        'name': "copy",
        'config': {
            'read_par': 4,
            'deser_par': 4,
            'comp_par': 2,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 2,
            'write_par': 4,
            'compression_lib': 'lz4',
            'buffer_size': 512,
            'server_buffpool_size': 4 * 512 * 20,
            'client_buffpool_size': 4 * 512 * 20,
            'format': 1
        }
    },
    {
        'name': "etl",
        'config': {
            'read_par': 8,
            'deser_par': 6,
            'comp_par': 6,
            'send_par': 4,
            'rcv_par': 4,
            'decomp_par': 6,
            'write_par': 8,
            'compression_lib': 'zstd',
            'buffer_size': 256,
            'server_buffpool_size': 15000,
            'client_buffpool_size': 2 * 256 * 20,
            'format': 2
        }
    },
    {
        'name': "pg",
        'config': {
            'read_par': 4,
            'deser_par': 3,
            'comp_par': 3,
            'send_par': 2,
            'rcv_par': 2,
            'decomp_par': 4,
            'write_par': 1,
            'compression_lib': 'zstd',
            'buffer_size': 256,
            'server_buffpool_size': 2 * 256 * 20,
            'client_buffpool_size': 2 * 256 * 20,
            'format': 1
        }
    }
]
