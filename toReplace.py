#!/usr/bin/env python
# coding:utf-8

config = None
logger = None


def get_config_logger(name, config=None):
    try: 
        # relative import
        from sys import path;path.extend("..")
        from common.Helpers.helpers import init_logger

        if config is None:
            config=name
        config, logger = init_logger(name=name, config=config)

    except:
        # Basic configuration
        import logging
        logging.basicConfig(level=logging.DEBUG)
        class Conf:
            DB_SERVER = "127.0.0.1"
            DB_NAME = "maindb"
            DB_USER = "dbuser"
            DB_PASSWORD = "dbuser"
            DB_PORT = 5432

        logger = logging.getLogger()
        config = Conf()
    return config, logger



