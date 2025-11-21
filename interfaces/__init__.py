#!/usr/bin/env python
# coding:utf-8

from .scraper import IScraper
from .data_source import IDataSource
from .data_backup import IDataBackup
from .data_saver import IDataSaver
from .calculator import ICalculator

__all__ = ['IScraper', 'IDataSource', 'IDataBackup', 'IDataSaver', 'ICalculator']