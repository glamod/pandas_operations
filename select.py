#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 09:48:18 2019

@author: iregon
"""
import operator #  we use it, but enclosed in an eval() method
import pandas as pd
from io import StringIO
from .common import logging_hdlr


def select_on_column(data,selection,how='eq',out_rejected = False):
#    how: 'eq','lt','gt','ge',....
#    The index of the resulting dataframe is reinitialized here, it does not
#    inherit from parent df
    def dataframe(df,col,value,comp,out_rejected = False):
        in_df = df.loc[comp(df[col],value)]
        in_df.index = range(0,len(in_df))
        if out_rejected:  
            out_df = df.loc[~comp(df[col],value)]
            out_df.index = range(0,len(out_df))
            return in_df,out_df
        else:
            return in_df

    def parser(parser,col,value,comp,out_rejected = False): 
        read_params = ['chunksize','names','dtype','parse_dates','date_parser',
                       'infer_datetime_format']
        read_dict = {x:parser.orig_options.get(x) for x in read_params}
        in_buffer = StringIO()
        if out_rejected:
            out_buffer = StringIO()    
        for df in parser:
            if out_rejected:
                in_df,out_df = dataframe(df,col,value,comp,out_rejected = True)
                in_df.to_csv(in_buffer,header = False, index = False, mode = 'a')
                out_df.to_csv(out_buffer,header = False, index = False, mode = 'a')
            else:
                in_df = dataframe(df,col,value,comp)
                in_df.to_csv(in_buffer,header = False, index = False, mode = 'a')
        
        in_buffer.seek(0)
        if out_rejected:
            out_buffer.seek(0)
            return pd.read_csv(in_buffer,**read_dict),pd.read_csv(out_buffer,**read_dict)
        else:
            return pd.read_csv(in_buffer,**read_dict)
   
    col = list(selection.keys())[0]
    value = list(selection.values())[0]
    comp = eval('operator.' + how)
    if not isinstance(data,pd.io.parsers.TextFileReader):
        df = dataframe(data,col,value,comp,out_rejected = out_rejected)
        return df
    else:
        parser = parser(data,col,value,comp,out_rejected = out_rejected)
        return parser
    
    return


def select_from_list(data,selection,out_rejected = False):
#    The index of the resulting dataframe is reinitialized here, it does not
#    inherit from parent df
    def dataframe(df,col,values,out_rejected = False):
        in_df = df.loc[df[col].isin(values)]
        in_df.index = range(0,len(in_df))
        if out_rejected:  
            out_df = df.loc[~df[col].isin(values)]
            out_df.index = range(0,len(out_df))
            return in_df,out_df
        else:
            return in_df
    
    def parser(parser,col,values,out_rejected = False): 
        read_params = ['chunksize','names','dtype','parse_dates','date_parser',
                       'infer_datetime_format']
        read_dict = {x:parser.orig_options.get(x) for x in read_params}
        in_buffer = StringIO()
        if out_rejected:
            out_buffer = StringIO()    
        for df in parser:
            if out_rejected:
                in_df,out_df = dataframe(df,col,values,out_rejected = True)
                in_df.to_csv(in_buffer,header = False, index = False, mode = 'a')
                out_df.to_csv(out_buffer,header = False, index = False, mode = 'a')
            else:
                in_df = dataframe(df,col,values)
                in_df.to_csv(in_buffer,header = False, index = False, mode = 'a')
        
        in_buffer.seek(0)
        if out_rejected:
            out_buffer.seek(0)
            return pd.read_csv(in_buffer,**read_dict),pd.read_csv(out_buffer,**read_dict)
        else:
            return pd.read_csv(in_buffer,**read_dict)
    
    col = list(selection.keys())[0]
    values = list(selection.values())[0]
    if not isinstance(data,pd.io.parsers.TextFileReader):
        df = dataframe(data,col,values,out_rejected = out_rejected)
        return df
    else:
        parser = parser(data,col,values,out_rejected = out_rejected)
        return parser
