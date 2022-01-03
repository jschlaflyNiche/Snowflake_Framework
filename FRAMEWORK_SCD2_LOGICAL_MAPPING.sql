create or replace procedure FRAMEWORK_SCD2_LOGICAL_MAPPING (TABLE_SCHEMA varchar, TABLE_NAME varchar, PK varchar, NULLS FLOAT)
returns varchar
language javascript
as

$$

var NULL_COUNT = ",null".repeat(NULLS.toFixed(NULLS));

var element_query = "select COLUMN_NAME from \n" +
                    "information_schema.columns where \n" +
                    "table_schema = " +  "'" + TABLE_SCHEMA + "'" + " and table_name = \n" +
                    "'" + TABLE_NAME + "'" + "and column_name <> 'UPDATE_TIMESTAMP' ORDER BY ORDINAL_POSITION ASC";


var element_stmt = snowflake.createStatement({sqlText:element_query});

var element_res = element_stmt.execute();

col_list = '';

while (element_res.next()) {

   if (col_list != "") {
      col_list += ", \n";}
   col_list += element_res.getColumnValue(1);
}


var view_name = TABLE_SCHEMA + "." + TABLE_NAME + "_change_data";
var stream_name = TABLE_SCHEMA + "_" + TABLE_NAME + "_change_stream";
var hist_table = TABLE_SCHEMA + "." + TABLE_NAME + "_history"


var query = "create or replace view " + view_name + " as select " + col_list + ", start_time, end_time, current_flag, 'I' as dml_type \n" +
             "from (select " + col_list +  " ,update_timestamp as start_time, \n" +
             "lag(update_timestamp) over (partition by " + PK + " order by update_timestamp desc) as end_time_raw, \n" +
             "case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time, \n" +
             "case when end_time_raw is null then 1 else 0 end as current_flag \n" +
             "from (select " + col_list + ", update_timestamp from " + stream_name + "\n" +
             "where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE')) \n" +
             "union select " + col_list + " , start_time, end_time, current_flag, dml_type \n" +
               "from (select " + col_list + ", update_timestamp as start_time, \n" +
                "lag(update_timestamp) over (partition by " + PK + " order by update_timestamp desc) as end_time_raw, \n" +
                "case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time, \n" +
                "case when end_time_raw is null then 1 else 0 end as current_flag,dml_type \n" +
                "from (select " + col_list +  ", update_timestamp, 'I' as dml_type \n" +
                "from " + stream_name + " where metadata$action = 'INSERT' and metadata$isupdate = 'TRUE' \n" +
                "union select " + PK +  NULL_COUNT + ", start_time, 'U' as dml_type \n" +
                "from " + hist_table + " where " + PK + " in (select distinct " + PK + "\n" +
                "from " + stream_name + " where metadata$action = 'INSERT' and metadata$isupdate = 'TRUE') and current_flag = 1)) \n" +
                "union select nms." + PK + NULL_COUNT + ", nh.start_time, current_timestamp()::timestamp_ntz, null, 'D' \n" +
                "from " + hist_table + " nh inner join " + stream_name + " nms \n" +
                "on nh." + PK + " = nms." + PK + " where nms.metadata$action = 'DELETE' and   nms.metadata$isupdate = 'FALSE' and  nh.current_flag = 1;"





var stmt3 = snowflake.createStatement({sqlText:query});
var creat_view = stmt3.execute();

return NULL_COUNT;


$$;
