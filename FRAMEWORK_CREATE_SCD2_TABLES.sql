create or replace procedure FRAMEWORK_CREATE_SCD2_TABLES (CREATE_STMT varchar, TABLE_NAME varchar, SCHEMA_NAME varchar, DATABASE_NAME varchar, PK varchar, NULLS FLOAT, RAW_CREATE varchar)
returns varchar
language javascript
as




$$
//

table_name = '"' + DATABASE_NAME + '"."' + SCHEMA_NAME + '"."' + TABLE_NAME + '" ';
hist_table_name = '"' + DATABASE_NAME + '"."' + SCHEMA_NAME + '"."' + TABLE_NAME + '_HISTORY" ';

// STAGE AND RAW GO UNDER DIFFERENT SCHEMA

stage_table_name = '"' + DATABASE_NAME + '".RAW_STAGE."' + TABLE_NAME + '_STAGE" ';
raw_table = '"' + DATABASE_NAME + '".RAW_STAGE."' + TABLE_NAME + '_RAW" ';

var base_table = "CREATE OR REPLACE TABLE " + table_name + CREATE_STMT + ", update_timestamp timestamp_ntz);" ;
var stream_stmt = "CREATE OR REPLACE STREAM " + '"' + DATABASE_NAME + '"."' + SCHEMA_NAME + '"."' + SCHEMA_NAME + "_" + TABLE_NAME + '_CHANGE_STREAM"'+ " on table " + table_name;
var hist_table = "CREATE OR REPLACE TABLE " + hist_table_name + CREATE_STMT + ", start_time timestamp_ntz, end_time timestamp_ntz, current_flag int);";
var stage_table = "CREATE OR REPLACE TABLE " + stage_table_name + CREATE_STMT + ");";
var base_table_raw = "CREATE OR REPLACE TABLE " + raw_table + RAW_CREATE + ", update_timestamp timestamp_ntz);" ;



var element_stmt = snowflake.createStatement({sqlText:base_table});
var element_res = element_stmt.execute();

var element_stmt2 = snowflake.createStatement({sqlText:stream_stmt});
var element_res2 = element_stmt2.execute();

var element_stmt3 = snowflake.createStatement({sqlText:hist_table});
var element_res3 = element_stmt3.execute();

var element_stmt4 = snowflake.createStatement({sqlText:stage_table});
var element_res4 = element_stmt4.execute();

var element_stmt6 = snowflake.createStatement({sqlText:base_table_raw});
var element_res6 = element_stmt6.execute();

// create the change tracking view !!

stored_proc = "CALL SC2_VIEW('" + SCHEMA_NAME + "', '" + TABLE_NAME + "', '" +  PK + "', '" + NULLS + "')"
var element_stmt5 = snowflake.createStatement({sqlText:stored_proc});
var element_res5 = element_stmt5.execute();

return;


$$;
