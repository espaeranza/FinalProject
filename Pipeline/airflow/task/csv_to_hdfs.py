from hdfs import InsecureClient
import os

def upload_csv_to_hdfs(local_path, hdfs_path):
    # HDFS 연결 설정
    hdfs_client = InsecureClient('http://<HDFS_HOST>:<HDFS_PORT>', user='<HDFS_USER>')

    # HDFS에 CSV 파일 업로드
    with open(local_path, 'rb') as local_file:
        hdfs_client.write(hdfs_path, local_file)

if __name__ == "__main__":
    # 로컬 CSV 파일 경로 및 HDFS 업로드 경로 지정
    local_csv_path = '/home/lab04/study/final_project/file.csv'
    hdfs_upload_path = '/path/in/hdfs/file.csv'

    # CSV 파일을 HDFS에 업로드
    upload_csv_to_hdfs(local_csv_path, hdfs_upload_path)