# HADOOP/spark 실행 후 진행

# rdd 주피터 노트북 14_Extract.ipynb
from hdfs import InsecureClient # hadoop 서버와 연결하는  client객체 모듈(hdfs 접근)
import requests # web 사용
import json # json 기능 사용
import datetime as dt
import logging

######################
## 전역변수 영역
######################
client=InsecureClient('http://localhost:9870',user='root')
## service key decoding된 key 사용
url = 'http://apis.data.go.kr/1352000/ODMS_COVID_04/callCovid04Api'
# service_key = '7E2bfvO9I4sNthgKkGQ317Fa599toGAa8AU0+V1vd3JuJW1k+Web3iaSbsp5PjKgNiPWdsfweZjLRVopDaQuLQ=='
service_key = 'azOlzHmE12kpPv/6L9CM1ylcu+fqgba5RR/xip8K1OQuncWE3OGzAW5hiE2oKb6fbaMFwkSf8YGy1sytUI5feA=='
file_dir = '/corona_data/patient/' #hdfs 저장 경로

######################
# 기능 모듈 영역
######################
def executeRestApi(method, url, headers, params):  
    
    # raise Exception('응답코드 : 500')  # 예외 테스트
    # err_num = 10/0 # 예외 테스트
    if method == "get":
        res = requests.get(url, params=params, headers=headers)
    else:
        res = requests.post(url, data=params, headers=headers)

    if res == None or res.status_code != 200:
        raise Exception('응답코드 : ' + str(res.status_code))
       
    return res.text

# 현재 날짜로부터 befor_day 만큼 이전의 날짜를 생성해주는 함수
def cal_std_day(befor_day):   
    x = dt.datetime.now() - dt.timedelta(befor_day)
    year = x.year
    month = x.month if x.month >= 10 else '0'+ str(x.month)
    day = x.day if x.day >= 10 else '0'+ str(x.day)  
    return str(year)+ '-' +str(month)+ '-' +str(day)

# 로그 함수
def create_log(log_name) :
    co_logger = logging.getLogger(log_name)
    f_handler = logging.FileHandler('./log/rest_api/'+cal_std_day(0)+'.log')
    co_logger.addHandler(f_handler) 
    return co_logger

def create_param(std_day) : 
    return {'serviceKey' : service_key
            ,'pageNo' : 1
            ,'numOfRows' : '500'
            ,'apiType' : 'JSON'
            ,'std_day' : cal_std_day(std_day)}

def save_patient(co_logger,start,end):
    for i in range(start,end) :
        params = create_param(i) # api 요청 파라미터 생성
        log_dict={
            "is_sucess":"Fail"
        ,"type":"corona_patient"
        ,"std_day":params['std_day']
        ,"params" :params
        }
        try:
            res = executeRestApi('get', url,{}, params) # 공공데이터 api에서 data 추출
            file_name='corona_patient_'+cal_std_day(i)+'.json' # 저장 파일명 생성
            client.write(file_dir+file_name,res,encoding='utf=8') # hdfs에 저장
        except Exception as e :
            log_dict['err_msg']= e.__str__() # Exception객체 내 에러메시지를 반환
            log_json = json.dumps(log_dict, ensure_ascill=False) # json형태로 기록
            co_logger.error(log_json) # 로그 파일에 기록

if __name__ == '__main__' :
    logger_co = create_log('corona_api')
    save_patient(logger_co,365,366)            