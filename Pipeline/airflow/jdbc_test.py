import pymysql
from datetime import date, datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import datetime as dt
import pandas as pd 
import findspark
from pyspark.sql import SparkSession

def create_conf() :
    conf_dw = {
        'url':'jdbc:mysql://localhost:3306/etlmysql?characterEncoding=utf8&serverTimezone=Asia/Seoul'
        ,'props':{
        'user':'bigMysql',
        'password':'bigMysql1234@'   
        }
    }

    conf_dm = {
        'url':'jdbc:mysql://localhost:3306/etlmysqlDM?characterEncoding=utf8&serverTimezone=Asia/Seoul'
        ,'props':{
        'user':'bigDM',
        'password':'bigDM1234@'   
        }
    }
    return [conf_dw, conf_dm]

# spark session 생성 후 반환 
def get_spark_session() :
    findspark.init()
    return SparkSession.builder.getOrCreate()

# db에서 전달된 table data를 추출
def find_data(config, table_name) :
    spark_sc = get_spark_session()
    return spark_sc.read.jdbc(url= config['url'], table=table_name, properties=config['props'])