CREATE EXTERNAL TABLE STUDENT(STU_ID INT PRIMARY KEY,STU_NAME VARCHAR,STU_TIME BIGINT) LOCATION 'kafka://test?bootstrap-servers=bigdata1:9092,bigdata2:9092,bigdata3:9092' TBLPROPERTIES '{"producer":{"acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer"}}'
CREATE EXTERNAL TABLE STUDENT_NEW(STU_ID INT PRIMARY KEY,STU_NAME VARCHAR,STU_TIME VARCHAR) LOCATION 'kafka://test1?bootstrap-servers=bigdata1:9092,bigdata2:9092,bigdata3:9092' TBLPROPERTIES '{"producer":{"acks":"1","key.serializer":"org.apache.storm.kafka.IntSerializer"}}'
CREATE FUNCTION GET_TIME AS 'example.stormsql.GetTime'
INSERT INTO STUDENT_NEW SELECT STU_ID,STU_NAME,GET_TIME(STU_TIME, 'yyyy年MM月dd日 HH:mm:ss') AS TIME_RECEIVED_TIMESTAMP FROM STUDENT