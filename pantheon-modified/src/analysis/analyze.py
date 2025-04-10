#!/usr/bin/env python

from os import path

import arg_parser
import context
from helpers.subprocess_wrappers import check_call


def main():
    args = arg_parser.parse_analyze()

    analysis_dir = path.join(context.src_dir, 'analysis')
    # plot = path.join(analysis_dir, 'plot.py')
    # report = path.join(analysis_dir, 'report.py')
    # norm = path.join(analysis_dir,'parse_them_all.py')
    network_oracle = path.join(analysis_dir, 'network_oracle.py')
    base_address = '/d1/ccBench/pantheon-modified/src/experiments/'
    

    # plot_cmd = ['python', plot]
    # report_cmd = ['python', report]
    # normalize_cmd = ['python', norm]
    network_oracle_cmd = ['python', network_oracle]

    for cmd in [network_oracle_cmd]:
        if args.data_dir:
            file_address = base_address + args.data_dir
            cmd += [file_address]
        if args.schemes:
            cmd += ['--schemes', args.schemes]
        if args.include_acklink:
            cmd += ['--include-acklink']

    # if args.data_dir:
    #     normalize_cmd+=['--datadir',args.data_dir]

    check_call(network_oracle_cmd)


if __name__ == '__main__':
    main()
