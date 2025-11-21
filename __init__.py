#!/usr/bin/env python
# coding:utf-8

from os.path import basename, abspath, dirname

# Get project name from parent directory of src folder
PROJECT_NAME = basename(abspath(dirname(dirname(__file__))))



from toReplace import get_config_logger

__all__ = ['get_config_logger', 'init_logger', 'PROJECT_NAME']



