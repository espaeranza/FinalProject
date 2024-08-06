스토리지의 97% 사용됨 … 소진 시 스토리지가 부족해 파일을 만들거나 수정하거나 업로드할 수 없게 됩니다. 3개월 동안 100GB 스토리지를 월 ₩2,400 ₩600에 이용하세요.
####
# spark를 cluster로 연결해서 sparkshell을 open해서 사용하는 프로그램이 아님
# spark session과 spark context는 자동 생성이 안됨 : 생성해서 사용해야 함(코드 필요)
# pyspark을 이용해서 생성
# 1. 기존 sparksession이 있으면 초기화 : findspark 모듈 필요
# 2. session 생성 pyspark.sql의 SparkSession 모듈 필요

#corona_transform_dw.py
# 코로나 현황 데이터 날짜별로 테이블에 insert
import pymysql
from datetime import date, datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import datetime as dt
import pandas as pd 
import findspark
from pyspark.sql import SparkSession

########################################################
# 전역변수 영역
# db 서버 정보와 db 정보 : url 키의 value
# 계정정보 : props
JDBC = {
      'url':'jdbc:mysql://localhost:3306/etlmysql?characterEncoding=utf8&serverTimezone=Asia/Seoul'
     ,'props':{
      'user':'bigMysql',
      'password':'bigMysql1234@'   
      }
}

#########################################################
# 함수 영역

# spark session 생성 후 객체 반환하는 함수
def get_spark_session() :
    findspark.init() # spark 관련 객체 초기화
    return SparkSession.builder.getOrCreate() # 기존 session이 있으면 사용 아니면 생성

# 코로나 환자 data hdfs->sparkdf로 load 하는 함수
# 특정 파일명을 파라미터로 전달받아서 df를 반환하는 함수
def get_data(fileN) : 
    file_name= 'hdfs://localhost:9000/corona_data/patient/'+str(fileN )
    spark_sc = get_spark_session()
    p_json = spark_sc.read.json(file_name, encoding='UTF-8')
    return p_json

# 현재 날짜로부터 befor_day 만큼 이전의 날짜를 생성해주는 함수
def cal_std_day(befor_day):   
    x = dt.datetime.now() - dt.timedelta(befor_day)
    year = x.year
    month = x.month if x.month >= 10 else '0'+ str(x.month)
    day = x.day if x.day >= 10 else '0'+ str(x.day)  
    return str(year)+ '-' +str(month)+ '-' +str(day)

### 특정일 하루의 json파일을 추출해서 필요 컬럼 생성 후 db에 insert 하는 함수
def transform_data() :
    file_name='corona_patient_'+cal_std_day(365)+'.json' # corona_extract.py에서 기준일이 365로 설정
    data=[]
    co_p_json = get_data(file_name)
    for rdd1 in co_p_json.select(co_p_json.items).toLocalIterator() :
        for rdd2 in rdd1.items :
            data.append(rdd2)

    tmp = get_spark_session().createDataFrame(data)

    co_p = tmp.select(
            tmp.gubun.alias('LOC'),
            tmp.deathCnt.alias('DEATH_CNT'),
            tmp.defCnt.alias('DEF_CNT'),
            tmp.localOccCnt.alias('LOC_OCC_CNT'),
            tmp.qurRate.alias('QUR_RATE'),
            tmp.stdDay.alias('STD_DAY'))\
            .where(~(col('LOC').isin(['합계','검역']))).distinct()
    return co_p

# db에 저장하는 역할
def dw_save() :
    tm_data = transform_data()
    tm_data.write.jdbc(url=JDBC['url'], table='CORONA_PATIENTS',mode='append',properties=JDBC['props'])

if __name__ == '__main__' :
    dw_save()