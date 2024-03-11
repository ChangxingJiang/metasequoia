# 从 Hive 类型到 DataX 类型的映射表：https://github.com/alibaba/DataX/blob/master/hdfsreader/doc/hdfsreader.md

HIVE_TO_DATAX = {
    "TINYINT": "Long",
    "SMALLINT": "Long",
    "INT": "Long",
    "BIGINT": "Long",
    "FLOAT": "Double",
    "DOUBLE": "Double",
    "STRING": "String",
    "CHAR": "String",
    "VARCHAR": "String",
    "STRUCT": "String",
    "MAP": "String",
    "ARRAY": "String",
    "UNION": "String",
    "BINARY": "String",
    "BOOLEAN": "Boolean",
    "DATE": "Date",
    "TIMESTAMP": "Date",
    "DECIMAL": "String",  # 映射表中不包含
}
