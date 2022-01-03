create or replace procedure FRAMEWORK_TRANSFORMED_DATA_MERGE (DATABASE_NAME varchar, SCHEMA_NAME varchar,TABLE_NAME varchar, PK varchar)
returns varchar
language javascript
as


$$

var target_tbl = '"' + DATABASE_NAME + '"."' + SCHEMA_NAME + '"."' + TABLE_NAME + '" ';
var stage_tbl = '"' + DATABASE_NAME + '".RAW_STAGE."' + TABLE_NAME + '_STAGE"';


var target_columns = "select COLUMN_NAME from \n" +
                    "information_schema.columns where \n" +
                    "table_schema = " +  "'" + SCHEMA_NAME + "'" + " and table_name = \n" +
                    "'" + TABLE_NAME + "'" + " and column_name <> 'UPDATE_TIMESTAMP' ORDER BY ORDINAL_POSITION ASC";



var source_columns = "select COLUMN_NAME from \n" +
                    "information_schema.columns where \n" +
                    "table_schema = " +  "'" + SCHEMA_NAME + "'" + " and table_name = \n" +
                    "'" + TABLE_NAME + "'" + " and column_name <> 'UPDATE_TIMESTAMP' ORDER BY ORDINAL_POSITION ASC";


var element_stmt1 = snowflake.createStatement({sqlText:target_columns});
var element_res1 = element_stmt1.execute();

target_cols = '';


while (element_res1.next()) {

   if (target_cols != "") {
      target_cols += ", \n";}
   target_cols += "t." + element_res1.getColumnValue(1);
}

var element_stmt2 = snowflake.createStatement({sqlText:source_columns});
var element_res2 = element_stmt2.execute();


source_cols = '';

while (element_res2.next()) {

   if (source_cols != "") {
      source_cols += ", \n";}
   source_cols += "s." + element_res2.getColumnValue(1);
}

var element_stmt3 = snowflake.createStatement({sqlText:source_columns});
var element_res3 = element_stmt3.execute();

source_cols2 = '';

while (element_res3.next()) {

   if (source_cols2 != "") {
      source_cols2 += ", \n";}
   source_cols2 += element_res3.getColumnValue(1);
}

var cols_zipped = "select listagg('t.' || COLUMN_NAME || ' = ' || 's.' || COLUMN_NAME, ' , ') as col from information_schema.columns \n" +
                    "where TABLE_SCHEMA = "  +  "'" + SCHEMA_NAME + "'" + " and table_name = \n" +
                     "'" + TABLE_NAME + "'" + " and column_name <> 'UPDATE_TIMESTAMP' and ORDINAL_POSITION <> 0";


var element_stmt4 = snowflake.createStatement({sqlText:cols_zipped});
var element_res4 = element_stmt4.execute();
element_res4.next()
var col_zip_list = element_res4.getColumnValue(1);




merge_stmt = "merge into " + target_tbl + " as t using \n" +
            "(SELECT " + source_cols2 + " FROM " + stage_tbl + ")\n" +
             "as s on t." + PK + " = s." + PK + " when matched and \n" +
             "hash(" + target_cols + ") <> hash(" + source_cols +
             ") then update set " + col_zip_list + ", t.update_timestamp = current_timestamp(2) \n" +
             "when not matched then insert (" + target_cols + " , t.update_timestamp) values (" + source_cols + " , current_timestamp(2))";



var element_stmt5 = snowflake.createStatement({sqlText:merge_stmt});
var element_res5 = element_stmt5.execute();



// ON SUCCESS DELETE THE STAGE TABLE !!!! HELLAS!


delete_stmt = "DELETE FROM " + stage_tbl
var element_stmt6 = snowflake.createStatement({sqlText:delete_stmt});
var element_res6 = element_stmt6.execute();

return 'compelte';


$$;
