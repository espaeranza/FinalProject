hdfs dfs -copyToLocal /home/lab04/study/file.csv /tmp/file.csv

cat /tmp/file.csv | mysql --database_url 'your_database_url' --table 'your_table_name'