pty_data = [
{"id":"1","datetime":"2023-01-01","value":"A"},
{"id":"1","datetime":"2023-01-04","value":"B"},
{"id":"1","datetime":"2023-01-05","value":"C"},
{"id":"2","datetime":"2023-01-01","value":"D"}]
mem_data = [
{"id":"1","datetime":"2023-01-02","value":"X"},
{"id":"1","datetime":"2023-01-03","value":"Y"},
{"id":"1","datetime":"2023-01-05","value":"Z"},
{"id":"1","datetime":"2023-01-06","value":"S"}]
df_pty = spark.createDataFrame(pty_data)  
df_mem = spark.createDataFrame(mem_data) 
df_pty.createOrReplaceTempView("pty")  
df_mem.createOrReplaceTempView("mem") 

sql_query = """
WITH DT_VIEW AS (
    SELECT DATETIME FROM PTY
    UNION
    SELECT DATETIME FROM MEM
),
FULL_VIEW AS(
    SELECT 
         IFNULL(T2.ID,T3.ID) AS ID
        ,T2.VALUE AS VALUE_1
        ,T3.VALUE AS VALUE_2
        -- ,T2.DATETIME AS DATETIME_1
        -- ,T3.DATETIME AS DATETIME_2
        ,IFNULL(T2.DATETIME,T3.DATETIME) DATETIME
    FROM DT_VIEW T1
    LEFT JOIN PTY T2 ON T1.DATETIME = T2.DATETIME
    LEFT JOIN MEM T3 ON T1.DATETIME = T3.DATETIME 
),
SCD_VIEW AS(
    SELECT 
     T1.ID
    ,T1.VALUE_1
    ,LAST_VALUE(T1.VALUE_1, TRUE) OVER (PARTITION BY id ORDER BY T1.DATETIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LAST_VALUE_1
    ,T1.VALUE_2
    ,LAST_VALUE(T1.VALUE_2, TRUE) OVER (PARTITION BY id ORDER BY T1.DATETIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LAST_VALUE_2
    ,T1.DATETIME START_DATE
    ,IFNULL(LEAD(T1.DATETIME) OVER (PARTITION BY T1.ID ORDER BY T1.DATETIME),'9999-01-01') END_DATE
FROM FULL_VIEW T1
ORDER BY T1.ID,T1.DATETIME
)
SELECT * FROM SCD_VIEW
"""
spark.sql(sql_query).show()
