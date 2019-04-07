# -*- coding: utf-8 -*-
"""
Created on Mon May 21 08:41:08 2018

@author: MichaelEK
"""
import pandas as pd
from pdsql import mssql
import os

############################################
### Parameters

server = 'edwprod01'
database = 'hydro'
ts_daily_table = 'TSDataNumericDaily'
ts_daily_summ_table = 'TSDataNumericDailySumm'
ts_hourly_table = 'TSDataNumericHourly'
ts_hourly_summ_table = 'TSDataNumericHourlySumm'

sites = ['71162', '1071105']
dataset = [5]
from_date = '2010-07-01'
to_date = '2018-06-30'

py_path = os.path.realpath(os.path.dirname(__file__))
data_dir = 'data'
export_flow = 'flow.csv'

############################################
### Get data

## Pull out recorder data
tsdata = mssql.rd_sql_ts(server, database, ts_hourly_table, 'ExtSiteID', 'DateTime', 'Value', where_in={'ExtSiteID': sites, 'DatasetTypeID': dataset}, from_date=from_date, to_date=to_date)

## Reformat
tsdata1 = tsdata.unstack(0)
tsdata1.columns = tsdata1.columns.droplevel(0)

## Save data
tsdata1.to_csv(os.path.join(py_path, data_dir, export_flow))



