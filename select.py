#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 09:48:18 2019

@author: iregon
"""
import operator #  we use it, but enclosed in an eval() method
import pandas as pd
from io import StringIO
from .common import TextParser_hdlr

# Need to define a general thing for the parser() functions, like we did with
# the dataframe_apply_index(), because they are all the same but for the
# selection applied!!!!!

#    The index of the resulting dataframe(s) is reinitialized here, it does not
#    inherit from parent df
#
#    data is a dataframe or a TextFileReader

def dataframe_apply_index(df,index_list,out_rejected = False,in_index = False,
                  idx_in_offset = 0,idx_out_offset = 0):

    index = df.index.isin(index_list)
    in_df = df[index]
    in_df.index = range(idx_in_offset,idx_in_offset + len(in_df))
    output = [in_df]
    if out_rejected:
        out_df = df[~index]
        out_df.index = range(idx_out_offset,idx_out_offset + len(out_df))
        output.append(out_df)
    if in_index:
        output.append(index_list)

    return output


def select_true(data,mask,col,out_rejected = False,in_index = False):
#   mask is a the full df/parser of which we only use col
    def dataframe(df,mask,out_rejected = False, in_index = False,
                  idx_in_offset = 0, idx_out_offset = 0):
        # get the index values and pass to the general function
        # If a mask is empty, assume True (...)
        index = mask[mask.fillna(True)].index
        return dataframe_apply_index(df,index ,out_rejected = out_rejected,
                  in_index = in_index, idx_in_offset = idx_in_offset,
                  idx_out_offset = idx_out_offset)


    def parser(data_parser,mask_parser,col,out_rejected = False, in_index = False):
        mask_cp = TextParser_hdlr.make_copy(mask_parser)
        read_params = ['chunksize','names','dtype','parse_dates','date_parser',
                       'infer_datetime_format']
        read_dict = {x:data_parser.orig_options.get(x) for x in read_params}
        in_buffer = StringIO()
        if out_rejected:
            out_buffer = StringIO()
        if in_index:
            index = []
        idx_in_offset = 0
        idx_out_offset = 0
        for df,mask_df in zip(data_parser,mask_cp):
            o = dataframe(df,mask_df[col],out_rejected = out_rejected,
                          in_index = in_index, idx_in_offset = idx_in_offset,
                          idx_out_offset = idx_out_offset)
            o[0].to_csv(in_buffer,header = False, index = False, mode = 'a')
            if out_rejected:
                o[1].to_csv(out_buffer,header = False, index = False, mode = 'a')
                idx_out_offset += len(o[1])
            if in_index and not out_rejected:
                index.extend(o[1])
            if in_index and out_rejected:
                index.extend(o[2])
            idx_in_offset += len(o[0])

        mask_cp.close()
        in_buffer.seek(0)
        output = [pd.read_csv(in_buffer,**read_dict)]
        if out_rejected:
            out_buffer.seek(0)
            output.append(pd.read_csv(out_buffer,**read_dict))
        if in_index:
            output.append(index)

        return output

    if not isinstance(data,pd.io.parsers.TextFileReader):
        output = dataframe(data,mask[col],out_rejected = out_rejected,
                       in_index = in_index)
    else:
        output = parser(data,mask,col,out_rejected = out_rejected,
                        in_index = in_index)

    if len(output) > 1:
        return output
    else:
        return output[0]

def select(data, selection, how='eq', out_rejected=False, in_index=False):
#    how: 'eq','lt','gt','ge',....
#   selection is a dictionary like {col_name:value}
    def dataframe(df,col,value,comp,out_rejected = False, in_index = False,
                  idx_in_offset = 0, idx_out_offset = 0):
        # get the index values and pass to the general function
        in_df = df.loc[comp(df[col],value)]
        index = list(in_df.index)
        return dataframe_apply_index(df,index ,out_rejected = out_rejected,
                  in_index = in_index, idx_in_offset = idx_in_offset,
                  idx_out_offset = idx_out_offset)

    def parser(data_parser, col, value, comp, out_rejected=False,
               in_index=False):
        read_params = ['chunksize', 'names', 'dtype', 'parse_dates', 
                       'date_parser', 'infer_datetime_format']
        read_dict = {x:data_parser.orig_options.get(x) for x in read_params}
        in_buffer = StringIO()
        if out_rejected:
            out_buffer = StringIO()
        if in_index:
            index = []
        idx_in_offset = 0
        idx_out_offset = 0
        for df in data_parser:
            o = dataframe(df, col, value, comp, out_rejected=out_rejected,
                          in_index = in_index, idx_in_offset = idx_in_offset,
                          idx_out_offset = idx_out_offset)
            o[0].to_csv(in_buffer,header = False, index = False, mode = 'a')
            if out_rejected:
                o[1].to_csv(out_buffer,header = False, index = False, mode = 'a')
                idx_out_offset += len(o[1])
            if in_index and not out_rejected:
                index.extend(o[1])
            if in_index and out_rejected:
                index.extend(o[2])
            idx_in_offset += len(o[0])

        in_buffer.seek(0)
        output = [pd.read_csv(in_buffer,**read_dict)]
        if out_rejected:
            out_buffer.seek(0)
            output.append(pd.read_csv(out_buffer,**read_dict))
        if in_index:
            output.append(index)

        return output

    col = list(selection.keys())[0]
    value = list(selection.values())[0]
    comp = eval('operator.' + how)
    if not isinstance(data, pd.io.parsers.TextFileReader):
        output = dataframe(data, col, value, comp, out_rejected=out_rejected,
                           in_index=in_index)
    else:
        output = parser(data, col, value, comp, out_rejected=out_rejected,
                        in_index=in_index)

    if len(output) > 1:
        return output
    else:
        return output[0]


def select_from_list(data, selection, out_rejected = False, in_index = False):
#   selection is a dictionary like {col_name:[values to select]}
    def dataframe(df, col, values, out_rejected = False, in_index = False,
                  idx_in_offset = 0, idx_out_offset = 0):
        # get the index values and pass to the general function
        in_df = df.loc[df[col].isin(values)]
        # in_df = df.loc[df[col].isin([int(i) for i in values])]
        index = list(in_df.index)
        return dataframe_apply_index(df, index, out_rejected=out_rejected,
                                     in_index=in_index, 
                                     idx_in_offset=idx_in_offset, 
                                     idx_out_offset=idx_out_offset)

    def parser(data_parser, col, values, out_rejected=False, in_index=False):
        read_params = ['chunksize','names','dtype','parse_dates','date_parser',
                       'infer_datetime_format']
        read_dict = {x:data_parser.orig_options.get(x) for x in read_params}
        in_buffer = StringIO()
        if out_rejected:
            out_buffer = StringIO()
        if in_index:
            index = []
        idx_in_offset = 0
        idx_out_offset = 0
        for df in data_parser:
            o = dataframe(df, col, values, out_rejected=out_rejected,
                          in_index=in_index, idx_in_offset=idx_in_offset,
                          idx_out_offset=idx_out_offset)
            o[0].to_csv(in_buffer, header=False, index=False, mode='a')
            if out_rejected:
                o[1].to_csv(out_buffer, header=False, index=False, mode='a')
                idx_out_offset += len(o[1])
            if in_index and not out_rejected:
                index.extend(o[1])
            if in_index and out_rejected:
                index.extend(o[2])
            idx_in_offset += len(o[0])

        in_buffer.seek(0)
        output = [pd.read_csv(in_buffer,**read_dict)]
        if out_rejected:
            out_buffer.seek(0)
            output.append(pd.read_csv(out_buffer,**read_dict))
        if in_index:
            output.append(index)

        return output

    col = list(selection.keys())[0]
    values = list(selection.values())[0]
    if not isinstance(data, pd.io.parsers.TextFileReader):
        output = dataframe(data, col, values, out_rejected=out_rejected, 
                           in_index=in_index)
    else:
        output = parser(data, col, values, out_rejected=out_rejected,
                        in_index=in_index)

    if len(output) > 1:
        return output
    else:
        return output[0]


def select_from_index(data,index,out_rejected = False):
    # index is a list of integer positions to select from data
    def dataframe(df,index,out_rejected = False,idx_in_offset = 0,
                  idx_out_offset = 0):

        return dataframe_apply_index(df,index,out_rejected = out_rejected,
                  idx_in_offset = idx_in_offset,
                  idx_out_offset = idx_out_offset)

    def parser(data_parser,index, out_rejected = False):
        read_params = ['chunksize','names','dtype','parse_dates','date_parser',
                       'infer_datetime_format']
        read_dict = {x:data_parser.orig_options.get(x) for x in read_params}
        in_buffer = StringIO()
        if out_rejected:
            out_buffer = StringIO()

        for df in data_parser:
            o = dataframe(df,index,out_rejected = out_rejected)
            o[0].to_csv(in_buffer,header = False, index = False, mode = 'a')
            if out_rejected:
                o[1].to_csv(out_buffer,header = False, index = False, mode = 'a')

        in_buffer.seek(0)
        output = [pd.read_csv(in_buffer,**read_dict)]
        if out_rejected:
            out_buffer.seek(0)
            output.append(pd.read_csv(out_buffer,**read_dict))
        return output

    if not isinstance(data,pd.io.parsers.TextFileReader):
        output =  dataframe(data,index,out_rejected = out_rejected)
    else:
        output =  parser(data,index,out_rejected = out_rejected)

    if len(output) > 1:
        return output
    else:
        return output[0]