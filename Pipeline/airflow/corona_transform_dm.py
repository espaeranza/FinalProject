스토리지의 97% 사용됨 … 소진 시 스토리지가 부족해 파일을 만들거나 수정하거나 업로드할 수 없게 됩니다. 3개월 동안 100GB 스토리지를 월 ₩2,400 ₩600에 이용하세요.
# corona_transform_dm.py : note 17번 코드 재구성
from datetime import date, datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import findspark
import datetime as dt
import pandas as pd
# from corona_transform_dw import get_spark_session  # 기존 생성한 함수 import 

## dw db 에서 data(table)추출 후 변환 후 dm db로 저장
## dw와 dm이 서로 다른 db 사용 jdbc 정보가 2개 : 함수로 구성

## 기준일 생성 함수
def cal_std_day(befor_day):   
    x = dt.datetime.now() - dt.timedelta(befor_day)
    year = x.year
    month = x.month if x.month >= 10 else '0'+ str(x.month)
    day = x.day if x.day >= 10 else '0'+ str(x.day)  
    return str(year)+ '-' +str(month)+ '-' +str(day)

# db정보 반환 함수
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

# db에서 전달된 table data 모두를 추출
def find_data(config, table_name) :
    spark_sc = get_spark_session()
    return spark_sc.read.jdbc(url= config['url'], table=table_name, properties=config['props'])

def find_query(config, query_t) :
    spark_sc = get_spark_session()
    df = spark_sc.read.format("jdbc")\
                    .option('url',config['url'])\
                    .option('driver','com.mysql.cj.jdbc.Driver')\
                    .option('query',query_t)\
                    .option('user',config['props']['user'])\
                    .option('password',config['props']['password'])\
                    .load()
    return df    

# 전달된 db를 db에 저장
def save_data(config, df, table_name) :
    return df.write.jdbc(url= config['url'], table=table_name, mode='append' , properties=config['props'])

# 코로나현황과 인구 관계 
def transdata_popPatient() :
    tmp_q = "select * from CORONA_PATIENTS where STD_DAY='" +str(cal_std_day(365)) +"'"
    conf = create_conf()
    popu = find_data(conf[0], 'LOC')
    patients = find_query(conf[0],tmp_q)

    ## 면적대비 인구수와 코로나 현황
    pop_patients=popu.join(patients, on='loc')\
                    .select('LOC',
                            ceil(col('POPULATION')/col('AREA')).alias('popu_density'),
                            'QUR_RATE','STD_DAY')\
                    .orderBy(col('STD_DAY'))
    
    # 각 행별로 고유 인덱스 컬럼 추가
    pop_patients= pop_patients.withColumn('CP_IDX',monotonically_increasing_id())
    # DM DB 에 저장 - 테이블에 레코드가 insert 됨
    save_data(conf[1],pop_patients,'CO_POPU_DENSITY')                                        

# 백신접종자수와 코로나환자와의 관계
def transdata_vaccinePatient() :
    tmp_q = "select * from CORONA_PATIENTS where STD_DAY='" +str(cal_std_day(365)) +"'"
    conf = create_conf()
    # 백신 data 추출
    vaccine = find_data(conf[0], 'CORONA_VACCINE')                                          
    # 지역별 인구 10만명당 백신 접종률 계산
    # 인구 data 필요
    popu = find_data(conf[0], 'LOC')
    # 코로나 현황 data 필요
    patients = find_query(conf[0],tmp_q) 
    ### long 형 df -> wide 형 df
    ### pivot 사용 : panas df로 변환 
    pd_vaccine = vaccine.to_pandas_on_spark()    
    pd_vaccine = pd_vaccine.pivot_table(index = ['LOC','STD_DAY'], columns='V_TH',values='V_CNT')
    pd_vaccine = pd_vaccine.reset_index()
    #sparkdf로 변환
    vaccine = pd_vaccine.to_spark() 
    # 인구 10만명당 백신 접종률 계산 - 3차 접종자 기준
    vac_rate = vaccine.join(popu,on='LOC')\
                    .select('LOC',
                            'STD_DAY',
                            ceil(col('V3')/col('POPULATION') * 100000).alias('Third_RATE'))
    co_vaccine = vac_rate.join(patients,on=['LOC','STD_DAY'])\
                     .select('LOC','STD_DAY','Third_RATE','QUR_RATE')
    # 기준키 컬럼 생성
    co_vaccine=co_vaccine.withColumn('CV_IDX',monotonically_increasing_id())
    save_data(conf[1], co_vaccine, 'CO_VACCINE_THIRD')

# 다중 이용시설과 코로나 확진자수 관계    
def transdata_FacPatient():
    tmp_q = "select * from CORONA_PATIENTS where STD_DAY='" +str(cal_std_day(365)) +"'"
    conf = create_conf()
    facil = find_data(conf[0], 'LOC_FACILITY_CNT')
    patients = find_query(conf[0],tmp_q)
    popu = find_data(conf[0], 'LOC') 
    
    # 인구 10만명당 다중 이용 시설의 수
    fac_popu = popu.join(facil, on='LOC')\
            .select('LOC',ceil(facil.FAC_CNT/popu.POPULATION*100000).alias('FAC_POPU'))
    co_facil = patients.join(fac_popu,on='LOC')\
                    .select('LOC','FAC_POPU','QUR_RATE','STD_DAY')
    # idx 컬럼 추가
    co_facil = co_facil.withColumn('CF_IDX',monotonically_increasing_id()) 

    save_data(conf[1], co_facil,'CO_FACILITY')

# 요일별 확진자수
# 1. 특정일(corona_extract.py에서 추출한 날짜) 코로나 발생 현황 data 추출
# 2. dmdb의 CO_WEEKDAY tbl에서 가장 최근 기준일의 레코드를 추출  
# 3. 1번 데이터의 요일 결정 전국 코로나 증가현황 계산 
# 4. 3번에서 계산한 특정요일의 코로나 증가현황을 특정 요일의 기존 수와 더하기(특정요일 data 누적 증가)         
def transdata_weekPatients() :
    tmp_q = "select * from CORONA_PATIENTS where STD_DAY='" +str(cal_std_day(365)) +"'" #1번 코드
    tmp_qd = "select * from CO_WEEKDAY where std_day=(select max(std_day) from CO_WEEKDAY)" #2번 코드
    conf = create_conf()
    patients = find_query(conf[0],tmp_q)
    week = find_query(conf[1], tmp_qd)
    week_p = patients.withColumn('DAY_OF_WEEK',dayofweek(col('STD_DAY'))) # 요일 결정 3번코드
    week_p = week_p.groupby(week_p.DAY_OF_WEEK).agg(sum(col('LOC_OCC_CNT')).alias('patients')) # 전국증가현황계산 3번코드
    week_p = week_p.withColumn('DAY_OF_WEEK', when(week_p.DAY_OF_WEEK == 1, 'MON') # 요일 숫자를 문자로 변경 3번코드
                                         .when(week_p.DAY_OF_WEEK == 2, 'TUE')
                                         .when(week_p.DAY_OF_WEEK == 3, 'WED')
                                         .when(week_p.DAY_OF_WEEK == 4, 'THE')
                                         .when(week_p.DAY_OF_WEEK == 5, 'FRI')
                                         .when(week_p.DAY_OF_WEEK == 6, 'SAT')
                                         .when(week_p.DAY_OF_WEEK == 7, 'SUN'))
    # week.show() # 요일별 발생현황 table
    # week_p.show() # 특정 날짜의 발생현황
    # 특정요일 추출(4번코드)
    tmp_col = week_p.select(week_p.DAY_OF_WEEK).collect()[0][0]
    # print(tmp_col) # 요일 data 추출
    # 위에서 추출한 요일의 data를 week에서 추출 후 신규발생현황과 더하기 (4번코드)
    tmp_val = week.select(tmp_col).collect()[0][0] + week_p.select('patients').collect()[0][0]
    # 위에서 계산한 수치를 tmp_col 요일에 반영
    # week.show()
    week = week.withColumn(tmp_col,lit(tmp_val)) # 기존 컬럼 변경
    # week.show()
    # week_p.withColumn('STD_DAY',current_date().cast('string')).show()
    week = week.withColumn('STD_DAY',lit(cal_std_day(365)))
    save_data(conf[1], week, 'CO_WEEKDAY')

if __name__=='__main__' :
    transdata_popPatient()
    transdata_vaccinePatient()
    transdata_FacPatient()
    transdata_weekPatients()   