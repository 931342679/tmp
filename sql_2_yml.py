import yaml
from pprint import pprint

template = """
data_processor_list:  
  - data_processor: dp_01  
    source_table_id: src_tbl_id  
    target_table_id: tgt_tbl_id
    source_tables:  
      - source_table: edap.stg_crm_cm.party  
        columns: "partyid, count(1) as cnt"
        alias: pty1  
        where: "party_id is not null"
        group_by: "party_id"  
        having: "count(1) > 1"
      - source_table: edap.stg_crm_cm.party  
        columns: "*"  
        alias: pty2  
        where: "party_id in (select distinct party_id from pty1)"  
        group_by: null  
        having: null
    final_dataframe:
      source_view: "pty2"
      columns: "*"
      where: null  
      group_by: null  
      having: null
"""

with open('test.yml', 'r') as file:
    data_processor_config = yaml.safe_load(file)


def generate_sql(config):
    if not config:
        return None

    def parse_source_query(all_source_config: dict):
        sql_queries = []
        for source_table_config in all_source_config["source_tables"]:
            columns = source_table_config["columns"]
            alias = source_table_config["alias"]
            source_table = source_table_config["source_table"]
            where_clause = source_table_config.get("where", "")
            group_by_clause = source_table_config.get("group_by", "")
            having_clause = source_table_config.get("having", "")
            sql = f"SELECT {columns} FROM {source_table}"
            if where_clause:
                sql += f" WHERE {where_clause}"
            if group_by_clause:
                sql += f" GROUP BY {group_by_clause}"
            if having_clause:
                sql += f" HAVING {having_clause}"
            sql_queries.append(f"\n{alias} AS (" + sql + ")")
        return sql_queries

    def parse_target_query(target_config: dict):
        columns = target_config["columns"]
        source_view = target_config["source_view"]
        where_clause = target_config.get("where", "")
        group_by_clause = target_config.get("group_by", "")
        having_clause = target_config.get("having", "")
        sql = f"\nSELECT {columns} FROM {source_view}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if group_by_clause:
            sql += f" GROUP BY {group_by_clause}"
        if having_clause:
            sql += f" HAVING {having_clause}"
        return sql

    config = [query for query in config if query["data_processor"] == "dp_01"]
    result = {}
    for query in config:
        ALL_SOURCE_QUERY = "WITH" + ",".join(parse_source_query(query))
        FINAL_QUERY = parse_target_query(query.get("final_dataframe"))
        result[query["data_processor"]] = ALL_SOURCE_QUERY + FINAL_QUERY
    return result


data_processor_list = data_processor_config.get("data_processor_list")
pprint(data_processor_list)
sql_queries = generate_sql(data_processor_list)
pprint(sql_queries)
