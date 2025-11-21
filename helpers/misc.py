#!/usr/bin/env python
# coding:utf-8

############### Convert text to value in dataframe #################
from pandas import to_numeric as pdTo_numeric
from numpy import nan as npNan
def convertDigits(val):
    try: return pdTo_numeric(val)
    except ValueError: 
        if val == "-": return npNan
        if "%" in val: return convertDigits(val.replace("%", ''))
        else : return val
############### Convert text to value in dataframe #################
####################################################################
