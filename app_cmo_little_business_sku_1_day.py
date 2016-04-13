#!/usr/bin/env python3
#===============================================================================
#
#         FILE: app_cmo_little_business_sku_1_day.py
#
#        USAGE: ./app_cmo_little_business_sku_1_day.py
#
#  DESCRIPTION: sku粒度近七日销量
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES: 
#       AUTHOR: 
#      COMPANY: 
#      VERSION: 1.0
#      CREATED: 20160413
#    SRC_TABLE: 
#    TGT_TABLE: 
#===============================================================================
import sys
import os
sys.path.append(os.getenv('HIVE_TASK'))
from HiveTask import HiveTask

ht = HiveTask()
sql = """

use app;

set mapred.output.compress=true; 
set hive.exec.compress.output=true; 
set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec; 

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table app_cmo_little_business_sku_1_day partition(dt = '""" + ht.data_day_str + """')
select 
   '""" + ht.data_day_str + """' as op_time     
  ,item_sku_id
  ,item_name  
  ,sum(sale_qtty)                as yx_quantity 
from gdm.gdm_m04_ord_det_sum
 where dt >= '""" + ht.oneday(days = -6, sep ='-') + """'
   and sale_ord_dt >= '""" + ht.oneday(days = -6, sep ='-') + """' and sale_ord_dt <= '""" + ht.data_day_str + """'
   and sale_ord_valid_flag = 1
   and split_status_cd <> 1
   and substr(ord_flag, 60, 3) = '014'
group by 
   item_sku_id
  ,item_name  
;  
"""

ht.exec_sql(schema_name = 'app', table_name = 'app_cmo_little_business_sku_1_day', sql = sql) 

#==============================================================================================
#   schema_name: 必选
#    table_name: 可选
#           sql: 必选
#    merge_flag: False (default)
#  lzo_compress: 可选 False (default)  
#lzo_index_path: 依赖lzo_compress可选，不需要warehouse，实例化了表后自动找到localtion
#                '' ,[''] 压缩整个表 
#                Normal,
#                /home/use/dd_edw/db/table
#                ['partition1','partition2']
#                ['dir1','dir2']
#               
#merge_part_dir: [](default) 整个表都检测小文件  
#                [partition1,partition2]
#      min_size: 128Mb
#----------------------------------------------------------------------------------------------
#      max_size: 250Mb
#---------------------------------------------------------------------------------------------
#ht.merge_small_file(db, table, partition = [], min_size = 128*1024*1024)
#ht.CreateIndex(db, table, path = 'Normal')
#===============================================================================================
