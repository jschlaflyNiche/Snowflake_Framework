create or replace procedure FRAMEWORK_AWS_S3_AUTO_INGEST (DATE varchar, TABLE_NAME varchar, SCHEMA_PATH varchar, TABLE_PATH varchar)
returns varchar
language javascript
as


$$
//

var folder_path = "/" + DATE + "/" + SCHEMA_PATH + "/public." + TABLE_PATH + "/";
var folder_path = folder_path.toLowerCase()

get_cols = "select 'TO_' || DATA_TYPE || '('|| '$1:' || COLUMN_NAME || ' ) as ' || COLUMN_NAME \n " +
            "from information_schema.columns where \n" +
            "table_schema = " +  "'RAW_STAGE'" + " and table_name = \n" +
            "'" + TABLE_NAME + "_RAW'" + " and column_name <> 'UPDATE_TIMESTAMP' ORDER BY ORDINAL_POSITION ASC";


var element_stmt = snowflake.createStatement({sqlText:get_cols});
var element_res = element_stmt.execute();

col_list = '';

while (element_res.next()) {

   if (col_list != "") {
      col_list += ", \n";}
   col_list += element_res.getColumnValue(1);
}

var col_list = col_list.replace(/TEXT/g, "VARCHAR");
var col_list = col_list.replace(/TIMESTAMP_NTZ/g, "TIMESTAMP");
var col_list = col_list.replace(/TO_VARIANT/g, "parse_json")
var col_list = col_list.toLowerCase()

copy_raw = "COPY INTO " + '"SNOWFLAKEPOC"."RAW_STAGE".' +  '"' + TABLE_NAME + '_RAW" \n' +
           "FROM (SELECT " + col_list + ", current_timestamp(2) UPDATE_TIMESTAMP" +
           " FROM @temp_stage" + folder_path + " (PATTERN => '.*part.*')) FORCE = TRUE;";


var element_stmt2 = snowflake.createStatement({sqlText:copy_raw});
var element_res2 = element_stmt2.execute();


return;

$$;
