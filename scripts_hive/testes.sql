CREATE  TABLE employee
 (

	name string,
	
	work_place ARRAY<string>,
	
	sex_age STRUCT<sex:string,age:int>,
	
	skills_score MAP<string,int>,
	
	depart_title MAP<string,ARRAY<string>>
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
;

LOAD DATA LOCAL INPATH '/opt/hive/dados/employee.txt'
OVERWRITE INTO TABLE employee;


LOAD DATA INPATH 'hdfs://172.17.0.2:50070/opt/employee.txt'
OVERWRITE INTO TABLE employee;


CREATE TABLE pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;

SHOW DATABASES;

SHOW TABLES;


SELECT *
from employee;


SELECT e.work_place 
from employee e ;


SELECT e.work_place[0], e.work_place[1] 
from employee e ;

SELECT e.sex_age.sex
from employee e ;

SELECT e.skills_score['DB']
FROM employee e 


DESCRIBE DATABASE default

