SHOW DATABASES;

CREATE DATABASE STAR_PREDICT;

SHOW DATABASES;

USE star_predict;

CREATE TABLE star_predict(
	category1 VARCHAR(50),
    category2 VARCHAR(50),
	category_url VARCHAR(200),
    product_name VARCHAR(200),
	product_url VARCHAR(200),
    review_cnt INT,
    product_rating FLOAT,
    review TEXT(60000),
    review_good VARCHAR(50),
    review_soso VARCHAR(50),
    review_bad VARCHAR(50)
);

DROP TABLE star_predict;

set global local_infile=1;
LOAD DATA LOCAL INFILE "C:/Users/Gun Won Park/KDT30/final_project/pipeline/data/data_ver2.csv"
INTO TABLE star_predict.star_predict
FIELDS TERMINATED BY ","   
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 LINES;