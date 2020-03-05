#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 10:34:56 2019

Module with functions to handle pd.io.parsers.TextFileReader objects.

@author: iregon
"""

import pandas as pd
import copy
from io import StringIO
from . import logging_hdlr

logger = logging_hdlr.init_logger(__name__,level = 'DEBUG')

read_params = ['chunksize','names','dtype','parse_dates','date_parser',
                       'infer_datetime_format','delimiter']

def make_copy(OParser):
    try:
        #OParser.f.seek(0) # make sure origin is complete
        NewRef = StringIO(OParser.f.getvalue())
        read_dict = {x:OParser.orig_options.get(x) for x in read_params}
        NParser = pd.read_csv( NewRef,**read_dict) 
        return NParser
    except Exception:
        logger.error('Failed to copy TextParser', exc_info=True)
        return 

def restore(Parser):
    try:
        Parser.f.seek(0)
        read_dict = {x:Parser.orig_options.get(x) for x in read_params}
        Parser = pd.read_csv( Parser.f, **read_dict) 
        return Parser
    except Exception:
        logger.error('Failed to restore TextParser', exc_info=True)
        return Parser

def is_not_empty(Parser):
    try:
        Parser_copy = make_copy(Parser)
    except Exception:
        logger.error('Failed to process input. Input type is {}'.format(type(Parser)), exc_info=True)
        return
    try:
        first_chunk = Parser_copy.get_chunk()
        Parser_copy.close()
        if len(first_chunk) > 0:
            logger.debug('Is not empty')
            return True
        else:
            return False
    except Exception:
        logger.debug('Something went wrong',exc_info=True)
        return False

def get_length(Parser):
    try:
        Parser_copy = make_copy(Parser)
    except Exception:
        logger.error('Failed to process input. Input type is {}'.format(type(Parser)), exc_info=True)
        return

    no_records = 0
    for df in Parser_copy:
        no_records += len(df)

    Parser_copy.close()      
    return no_records
