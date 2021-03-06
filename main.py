#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 wanghao <wanghao054@chinasoftinc.com>
#
# Distributed under terms of the MIT license.

"""
oracle to oracle varify tool
"""

import logging
from app.controllers.get_oracle_data import collect_oracle_init
from app.controllers.get_oracle_table_primarykey import GetOracleTablePrimaryKey
from app.controllers.verify_source_and_dest_tables import VerifySourceAndDestTables

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s: %(message)s')


def init():
    # collect_oracle_init()
    # GetOracleTablePrimaryKey()
    VerifySourceAndDestTables()


if __name__ == '__main__':
    init()
