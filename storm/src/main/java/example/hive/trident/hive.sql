CREATE TABLE `test`.`tridenthivetopology`
(
    `id`     STRING,
    `name`   STRING,
    `phone`  STRING,
    `street` STRING
) PARTITIONED BY (`city` STRING,`state` STRING)
    CLUSTERED BY (id) INTO 4 buckets
 STORED AS ORC
TBLPROPERTIES("transactional"="true")