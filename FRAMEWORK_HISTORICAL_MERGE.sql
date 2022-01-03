create or replace procedure FRAMEWORK_HISTORICAL_MERGE (TABLE_SCHEMA varchar, TABLE_NAME varchar, PK varchar)
returns varchar
language javascript
as

$$

var hist_table = 'SNOWFLAKEPOC.' + TABLE_SCHEMA + "." + TABLE_NAME + "_HISTORY"
var view_name = 'SNOWFLAKEPOC.' + TABLE_SCHEMA + "." + TABLE_NAME + "_change_data"



var element_query2 = "select COLUMN_NAME from \n" +
                    "information_schema.columns where \n" +
                    "table_schema = " +  "'" + TABLE_SCHEMA + "'" + " and table_name = \n" +
                    "'" + TABLE_NAME + "_HISTORY'" + "ORDER BY ORDINAL_POSITION DESC";


var element_stmt2 = snowflake.createStatement({sqlText:element_query2});

var element_res = element_stmt2.execute();
var element_res2 = element_stmt2.execute();


col_list1 = '';
col_list2 = '';



while (element_res.next()) {

   if (col_list1 != "") {
      col_list1 += ", \n";}
   col_list1 += "m." + element_res.getColumnValue(1);
}

while (element_res2.next()) {

   if (col_list2 != "") {
      col_list2 += ", \n";}
   col_list2 += element_res2.getColumnValue(1);
}


merge_stmt = "merge into " + hist_table + " nh \n" +
"using " + view_name + " m \n" +
"on nh." + PK + " = m." + PK + "\n" +
   "and nh.start_time = m.start_time \n" +
"when matched and m.dml_type = 'U' then update \n" +
    "set nh.end_time = m.end_time, \n" +
        "nh.current_flag = 0" +
"when matched and m.dml_type = 'D' then update \n" +
  "set nh.end_time = m.end_time, \n" +
       "nh.current_flag = 0 \n" +
"when not matched and m.dml_type = 'I' then insert \n" +
     "(" + col_list2 + ") \n" +
    "values (" + col_list1 + ")";


var merge_stmt = snowflake.createStatement({sqlText:merge_stmt});
var merge = merge_stmt.execute();



$$;
