--DROP PROCEDURE operations.sp_compare_table(varchar(4000),varchar(4000),varchar(4000),varchar(4000),varchar(4000),varchar(4000),varchar(4000),boolean);

--Get Doc
--call operations.sp_compare_table(null,null,null,null,null,null,null,null,null)

CREATE SCHEMA IF NOT EXISTS operations
;

CREATE OR REPLACE PROCEDURE operations.sp_compare_table 
  ( 
     v_SchemaTable1 varchar(4000)
    ,v_SchemaTable2 varchar(100)
    ,v_PKFields varchar(4000)
    ,v_CompareFields varchar(4000)
    ,v_ExcludeFields varchar(4000)
    ,v_FilterCondition1 varchar(4000)
    ,v_FilterCondition2 varchar(4000)    
    ,v_include_sql boolean
    ,v_scale int    
  )
AS
$$ DECLARE

----------------------------------
--row record
----------------------------------
row record;

----------------------------------
--Flags
----------------------------------
v_Flag_Schema1_is_spectrum int = 0;
v_Flag_Schema2_is_spectrum int = 0;

----------------------------------
--Parsed parameters
----------------------------------
v_Schema1 varchar(max);
v_Table1 varchar(max);
v_Schema2 varchar(max);
v_Table2 varchar(max);

----------------------------------
--Strings to build command
----------------------------------
v_ColList varchar(max) := '';
v_ColList_cmd varchar(max) := '';
v_select_cmd varchar(max) := '';
v_PKJoin_cmd varchar(max) := '';
v_from_cmd varchar(max) := '';
v_from_outer_cmd varchar(max) := '';
v_where_cmd varchar(max) := '';
v_sql_cmd varchar(max) := '';
v_sql_cmd_format varchar(max) := '';
v_string varchar(max) := '';
v_buildstring varchar(max) := '';
v_union_cmd varchar(max) := '';
v_FilterCondition varchar(max) := '';
v_MissingColList varchar(max) := '';
v_outer_col_list varchar(max) := '';
v_scale_str varchar(max) := '';
v_ColList_diff varchar(max) := '';
----------------------------------
--Counters
----------------------------------
v_cntr int := 1;
v_Wordcnt int := 0;

BEGIN 
IF v_SchemaTable1 IS NULL OR v_SchemaTable1 IS NULL OR ISNULL (v_PKFields,'') = '' THEN raise info '
*********************************************************************************************************************************************************
DESCRIPTION:
Compares any two tables/views. Outputs 4 temp tables that show differences:
1. tmp_compare --> Straight join between the two tables with columns side by side and added "diff" column. 
2. tmp_compare_unpivot --> Unpivotted table consisting of PK fields, column name, and diff
3. tmp_missing_t2 --> Records that are in table 1 but not in table 2
4. tmp_missing_t1 --> Records that are in table 2 but not in table 1

 
PARAMETERS:
v_SchemaTable1 (required) = Name of first table or view to compare. The following are allowed: internal table, spectrum table, view on internal tables.
v_SchemaTable2 (required) = Name of second table or view to compare. The following are allowed: internal table, spectrum table, view on internal tables.
v_PKFields (required)= Comma separated List of Primary Key columns
v_CompareFields (not required) = Comma separated list of columns to compare. Use ''*'' or NULL to compare all none primary keys fields
v_Excludefields (not required)= don''t include these fields in the compare
v_FilterCondition1 (not required)= filter for table 1 (in format "t1.column_nameX = ''XYZ'' and t1.column_name2 = ''123''")
v_FilterCondition2 (not required)= filter for table 2 (in format "t2.column_nameX = ''XYZ'' and t2.column_name2 = ''123''")
v_include_sql (not required) = true means the SQL statement to produce the tables will be included, default false
v_scale (not required) = round numerics and compare results to this scale prior to comparing. (e.g. scale of 2 rounds 2.123  to 2.12). Defaults to 100.

TODO:
1. Add support for mixing internal and external tables
2. Add parameter to filter percent diff
3. v_scale logic should be updated. Remove rounding on compare output and instead cast the rounded value to a decimal. (NULL v_scale should continue to use float)
4. Use EXCEPT to show missing data instead of left join / check which is faster

EXAMPLES:
call operations.sp_compare_table   
(
''operations.table_1''                                                 --v_SchemaTable1
,''operations.table_2''                                                --v_SchemaTable2
,''pkfield''                                                        --v_PKFields
,null                                                               --v_CompareFields 
,''ignorefield''                                                    --v_ExcludeFields
,''t1.snapshot_date=''''19720109''''''                              --v_FilterCondition1                        
,''t2.snapshot_date=''''19720109''''''                              --v_FilterCondition2                        
,true                                                               --v_include_sql  
,2                                                                  --v_scale
);

HISTORY:
BAMERSI           20220130          Created
BAMERSI           20220325          Added support for spectrum tables and error handling around views referencing spectrum tables.
BAMERSI           20220325          Added left joins for missing data
BAMERSI           20220806          Added or isnull(v_CompareFields,''*'')=''*'' so v_CompareFields can be null or * to compare all fields

TODO:



*********************************************************************************************************************************************************
';
  return;
end if;

/******************************************************************************************** Parse Params ************************************************************************************************************/ 
select split_part(v_SchemaTable1,'.',1) into v_Schema1;
select split_part(v_SchemaTable1,'.',2) into v_Table1;
select split_part(v_SchemaTable2,'.',1) into v_Schema2;
select split_part(v_SchemaTable2,'.',2) into v_Table2;

--------------------------------------------------------------------------------------
--Check if this is a spectrum table compare
--------------------------------------------------------------------------------------
if exists (select 1 from svv_external_columns where schemaname = v_Schema1)
then
  select 1 into v_Flag_Schema1_is_spectrum;
  raise info '% is a spectrum schema',v_Schema1;
end if;  

if exists (select 1 from svv_external_columns where schemaname = v_Schema2)
then
  select 1 into v_Flag_Schema2_is_spectrum;
  raise info '% is a spectrum schema',v_Schema2;  
end if;  


--Report if spectrum or not
if (v_Flag_Schema1_is_spectrum <> v_Flag_Schema2_is_spectrum)
then
  raise info 'Cannot compare tables because one is internal and the other is spectrum. Only tables of the same type can be compared in this version of sp_Compare_table.';
  raise info 'Tip: insert the spectrum table into an internal table to compare.';
  return;
end if;


----------------------------------------------------------------------------------------------------------------------
--If views are being compared they cannot reference a spectrum table because columns cannot be determined
----------------------------------------------------------------------------------------------------------------------
if exists 
(
  select 1 from 
  information_schema.tables 
  where (lower(table_name) = lower(v_Table1) and lower(table_schema)=lower(v_Schema1) and table_type = 'VIEW') 
  or (lower(table_name) = lower(v_Table2) and lower(table_schema)=lower(v_Schema2) and table_type = 'VIEW')
)
then
  raise info ' ';
  raise info 'WARNING: At least one of the input tables is a view. Only views referencing internal tables can be compared.';
  raise info 'This is because column metadata for views referencing internal tables cannot be obtained.';  
  raise info 'Tip: use the base spectrum table or insert the spectrum view into an internal table to compare.';
end if;

---------------------------------------------------
--Combine filter commands into one
---------------------------------------------------
if v_FilterCondition1 is not null and v_FilterCondition2 is null
then
  select v_FilterCondition1 into v_FilterCondition;
elsif v_FilterCondition1 is null and v_FilterCondition2 is not null
then
  select v_FilterCondition2 into v_FilterCondition;
else
  select v_FilterCondition1 || ' and ' || v_FilterCondition2 into v_FilterCondition;
end if;

---------------------------------------------------
--Set v_scale_str
---------------------------------------------------
select cast(isnull(v_scale,100) as varchar(50)) into v_scale_str;

/**************************************************************************************** Build v_select_cmd ************************************************************************************************************/ 
--Data from pg_table_def on the leader node cannot be easily combined with user data that exists on compute nodes
--The only way I found how to do it is to create a cursor that pulls the data from pg_views

----------------------------------
--Ensure we can see the metadata
----------------------------------
if v_Flag_Schema1_is_spectrum=0 --Can only set search_path for internal schema
then
  select 'set search_path to ''$user'', ' || v_Schema1 || ',' || v_Schema2 into v_sql_cmd;
  raise info 'Setting search path as follows: %',v_sql_cmd;
  execute v_sql_cmd;
end if;  

--------------------------------------------------------------------
--Parse v_PKFields
--------------------------------------------------------------------
drop table if exists tmp_cols_to_include;
create temp table tmp_cols_to_include(col varchar(max))
;
insert into tmp_cols_to_include
with recursive numbers(NUMBER) as
(
select 1 UNION ALL
select NUMBER + 1 from numbers where NUMBER < 100
)
select split_part(v_PKFields,',',number) as "col"
from numbers
where split_part(v_PKFields,',',number) <> '';

--------------------------------------------------------------------
--Parse v_CompareFields & add to tmp_cols_to_include
--------------------------------------------------------------------
if isnull(v_CompareFields,'*')<>'*' then
  insert into tmp_cols_to_include
  with recursive numbers(NUMBER) as
  (
  select 1 UNION ALL
  select NUMBER + 1 from numbers where NUMBER < 100
  )
  select split_part(v_CompareFields,',',number) as "col"
  from numbers
  where split_part(v_CompareFields,',',number) <> '';
end if;

--------------------------------------------------------------------
--Parse v_ExcludeFields
--------------------------------------------------------------------
drop table if exists tmp_cols_to_exclude;
create temp table tmp_cols_to_exclude as
with recursive numbers(NUMBER) as
(
select 1 UNION ALL
select NUMBER + 1 from numbers where NUMBER < 100
)
select split_part(v_ExcludeFields,',',number) as "col"
from numbers
where split_part(v_ExcludeFields,',',number) <> '';

----------------------------------
--Get list of cols
----------------------------------
drop table if exists tmp_cols;
create temp table tmp_cols
(
      rid int
      ,pk_flag int
      ,schemaname varchar
      ,tablename varchar
      ,col varchar
      ,dtype varchar
      ,dtypecatetgory varchar
      ,buildstring1 varchar(max)
      ,buildstring2 varchar(max)
      ,buildstringCompare varchar(max)
      ,buildstringComplete varchar(max)
      ,buildstringUnion varchar(max)
);

------------------------------------------------------------------------
--Get fields if comparing non-spectrum tables
------------------------------------------------------------------------
if v_Flag_Schema1_is_spectrum=0 then
    raise info 'Getting non spectrum fields ...';
    for row in 
          select 
                schemaname
                ,tablename
                ,cast("column" as varchar(max)) as "col"
                ,cast("type" as varchar(max)) as "dtype"
                ,case 
                      when lower("type") like 'character%'then 'CHAR'
                      when lower("type") in ('integer','bigint','double precision','smallint','real') then 'NUM'                  
                      when lower("type") like 'decimal%' then 'NUM'     
                      when lower("type") like 'numeric%' then 'NUM'                         
                      when lower("type") in ('date') then 'DATE'                       
                      when lower("type") in ('timestamp without time zone') then 'TIME'                                         
                      else 'CHAR'                  
                 end as "dtypecatetgory"            
          from pg_table_def a
          where (lower(schemaname) = lower(v_Schema1) and lower(tablename)=lower(v_Table1))
          and exists (select 1 from pg_table_def b where lower(b.schemaname) = lower(v_Schema2) and lower(b.tablename)=lower(v_Table2) and b."column" = a."column")
    loop
      if (exists (select 1 from tmp_cols_to_include where col = row.col) or isnull(v_CompareFields,'*')='*')--Include fields
         and not exists (select 1 from tmp_cols_to_exclude  where col = row.col)--Excluded fields
        then
          --DEBUG: 
          raise info '%.% - % - % - %',row.schemaname,row.tablename,row.col,row.dtype,row.dtypecatetgory;
          insert into tmp_cols(rid,pk_flag,schemaname,tablename,col,dtype,dtypecatetgory,buildstring1,buildstring2,buildstringCompare,buildstringComplete) 
          values (v_cntr,0,row.schemaname,row.tablename,row.col,row.dtype,row.dtypecatetgory,'','','','');
          v_cntr = v_cntr + 1;
      end if;
    end loop;
------------------------------------------------------------------------
--Get fields if comparing spectrum tables
------------------------------------------------------------------------
else
raise info 'Getting spectrum fields ...';
    for row in 
          select 
                schemaname
                ,tablename
                ,cast("columnname" as varchar(max)) as "col"
                ,cast("external_type" as varchar(max)) as "dtype"
                ,case 
                      when lower("external_type") in ('bigint','double','float','int','smallint','numeric') then 'NUM'                  
                      when lower("external_type") like 'decimal%' then 'NUM'     
                      when lower("external_type") like 'numeric%' then 'NUM'                       
                      when lower("external_type") ='date' then 'DATE'                       
                      when lower("external_type") ='timestamp' then 'TIME'                                         
                      else 'CHAR'                  
                 end as "dtypecatetgory"            
          from svv_external_columns a
          where (lower(schemaname) = lower(v_Schema1) and lower(tablename)=lower(v_Table1))
          and exists (select 1 from svv_external_columns b where lower(b.schemaname) = lower(v_Schema2) and lower(b.tablename)=lower(v_Table2) and b."columnname" = a."columnname")
    loop
      if (exists (select 1 from tmp_cols_to_include where col = row.col) or isnull(v_CompareFields,'*')='*')--Include fields
         and not exists (select 1 from tmp_cols_to_exclude  where col = row.col)--Excluded fields
        then
          --DEBUG: 
          raise info '%.% - % - % - %',row.schemaname,row.tablename,row.col,row.dtype,row.dtypecatetgory;
          insert into tmp_cols(rid,pk_flag,schemaname,tablename,col,dtype,dtypecatetgory,buildstring1,buildstring2,buildstringCompare,buildstringComplete) 
          values (v_cntr,0,row.schemaname,row.tablename,row.col,row.dtype,row.dtypecatetgory,'','','','');
          v_cntr = v_cntr + 1;
      end if;
    end loop;
end if
;

----------------------------------------------------------------------------------------------------
--Abort If any provided fields do not exist
----------------------------------------------------------------------------------------------------
if exists(select 1 from tmp_cols_to_include where col not in (select col from tmp_cols))
then
  select listagg(col,',') from tmp_cols_to_include where col not in (select col from tmp_cols) into v_MissingColList;
  raise info ' ';
  raise info '******************************************** One or more input columns do not exist ********************************************';
  raise info 'Column(s): %',v_MissingColList;
  return;
end if;

------------------------
--Flag PKs
------------------------
select 'update tmp_cols set pk_flag=1 where charindex(col,''' || v_PKFields || ''')>0' into v_sql_cmd;
if isnull(len(v_sql_cmd),0)=0 
then
  raise info ' ';
  raise info '******************************************** Not able to Flag PKs (v_sql_cmd is empty). ********************************************';
  return;
end if;
execute v_sql_cmd;

------------------------------------------------------------------------------------------------------------------------------------------------
--CHECK Columns to compare
------------------------------------------------------------------------------------------------------------------------------------------------
select listagg(col,',') within group(order by pk_flag desc, rid) into v_ColList from tmp_cols where pk_flag=0;

--Print column list and PK Fields:
raise info ' ';
raise info '----Columns to compare----';
raise info '%', v_ColList;

raise info ' ';
raise info '----PK Fields----';
raise info '%', v_PKFields;

--If we have no columns, abort
if isnull(len(v_ColList),0)=0 then
  raise info ' ';
  raise info '******************************************** No columns to compare. ********************************************';
  raise info 'Tip: If you are comparing a view referencing a spectrum table column metadata cannot be obtained so use the base spectrum table instead or insert the data to an internal table first.';
  return;
end if;

------------------------
--Set Build Strings
------------------------
update tmp_cols set buildstring1 = 't1."' || col || '"' where pk_flag=1;
update tmp_cols set buildstring1 = 't1."' || col || '" as "t1_' || col ||'"' where pk_flag=0;
update tmp_cols set buildstring2 = 't2."' || col || '" as "t2_' || col ||'"' where pk_flag=0;
update tmp_cols set buildstring1 = ',' || buildstring1 where rid not in (select min(rid) from tmp_cols where pk_flag=1);
update tmp_cols set buildstring2 = ',' || buildstring2 where pk_flag=0;

---------------------------
--Set Compare of numerics
---------------------------
--          else abs(1-(cast(t1.' || col || ' as float) / nullif(cast(t2.' || col || ' as float),0))) 

update tmp_cols set buildstringCompare
= ',case  when cast(round(t1."' || col || '",' || v_scale_str || ') as float)=0 and cast(round(t2."' || col || '",' || v_scale_str || ') as float)=0 then 0
          when cast(round(t1."' || col || '",' || v_scale_str || ') as float)<>0 and cast(round(t2."' || col || '",' || v_scale_str || ') as float)=0 then 1
          when t1."' || col || '" is null and t2."' || col || '" is not null then 1          
          when t1."' || col || '" is not null and t2."' || col || '" is null then 1                    
          when t1."' || col || '" is null and t2."' || col || '" is null then 0
          else round(1-(cast(round(t1."' || col || '",' || v_scale_str || ') as float) / nullif(cast(round(t2."' || col || '",' || v_scale_str || ') as float),0)),' || v_scale_str || ')
          end as "diff_' || col || '"'
where dtypecatetgory='NUM'
and pk_flag=0;

---------------------------
--Set Compare of Strings
---------------------------
update tmp_cols set buildstringCompare
= ',case 
          when t1."' || col || '" is null and t2."' || col || '" is not null then 1          
          when t1."' || col || '" is not null and t2."' || col || '" is null then 1                    
          when t1."' || col || '" is null and t2."' || col || '" is null then 0
          when isnull(t1."' || col || '",''N/A'') <> isnull(t2."' || col || '",''N/A'') then 1 
          else 0 
          end as "diff_' || col || '"'
where dtypecatetgory ='CHAR'
and pk_flag=0;

---------------------------
--Set Compare of Date
---------------------------
update tmp_cols set buildstringCompare
= ',case 
          when t1."' || col || '" is null and t2."' || col || '" is not null then 1          
          when t1."' || col || '" is not null and t2."' || col || '" is null then 1                    
          when t1."' || col || '" is null and t2."' || col || '" is null then 0
          when isnull(t1."' || col || '",''19000101'') <> isnull(t2."' || col || '",''19000101'') then 1 
          else 0 
          end as "diff_' || col || '"'
where dtypecatetgory ='DATE'
and pk_flag=0;

---------------------------
--Set Compare of Time
---------------------------
update tmp_cols set buildstringCompare
= ',case           
          when t1."' || col || '" is null and t2."' || col || '" is not null then 1          
          when t1."' || col || '" is not null and t2."' || col || '" is null then 1                    
          when t1."' || col || '" is null and t2."' || col || '" is null then 0
          when isnull(t1."' || col || '",''19000101 00:00'') <> isnull(t2."' || col || '",''19000101 00:00'') then 1 
          else 0 
          end as "diff_' || col || '"'
where dtypecatetgory ='TIME'
and pk_flag=0;

------------------------
--Build v_select_cmd
------------------------
update tmp_cols set buildstringComplete = buildstring1 || buildstring2 || buildstringCompare;
select listagg(buildstringComplete,' ') within group(order by pk_flag desc, rid) into v_select_cmd from tmp_cols;

------------------------------------------------
--If we have no columns, abort
------------------------------------------------
if v_select_cmd is null then
  raise info ' ';
  raise info 'Unable to build select STMT.';
  return;
end if;

--------------------------------------------------------------------------------------------------------------------------------------------------------
--Build v_select_cmd
--------------------------------------------------------------------------------------------------------------------------------------------------------
select 'select ' || v_select_cmd into v_select_cmd;

--If we have no SELECT, abort
if isnull(len(v_select_cmd),0)=0 then
  raise info ' ';
  raise info '******************************************** Not able to build SELECT stmt (v_select_cmd). ********************************************';
  return;
end if;

/******************************************************************************************** Build v_PKJoin_cmd ************************************************************************************************************/ 
select 1 into v_cntr;
select regexp_count(v_PKFields,',') + 1 into v_Wordcnt;
while v_cntr <= v_Wordcnt
loop
      select split_part(v_PKFields,',',v_cntr) into v_string;
      select 't1.' || v_string || ' = t2.' || v_string || ' and ' || v_buildstring into v_buildstring;
      v_cntr = v_cntr + 1;
end loop;
--Remove trailing "and"
select 'on ' || left(v_buildstring,len(v_buildstring)-5) into v_PKJoin_cmd;


--If we have no v_PKJoin_cmd, abort
if isnull(len(v_PKJoin_cmd),0)=0 then
  raise info ' ';
  raise info '******************************************** Not able to build PK Join stmt (v_PKJoin_cmd). ********************************************';
  return;
end if;

/******************************************************************************************** Build v_from_cmd ************************************************************************************************************/ 
select ' from ' || v_SchemaTable1 || ' t1 inner join ' || v_SchemaTable2 || ' t2 ' || v_PKJoin_cmd INTO v_from_cmd;


/******************************************************************************************** Build v_where_cmd ************************************************************************************************************/ 
if v_FilterCondition is not null then
  select ' where ' || v_FilterCondition into v_where_cmd;
end if;

/******************************************************************************* Build SQL: Pivot Prep CMD ************************************************************************************************************/ 
drop table if exists tmp_compare_prep;
select 'create temp table tmp_compare_prep as ' || v_select_cmd || v_from_cmd || v_where_cmd into v_sql_cmd;

--If we have no CMD, abort
if isnull(len(v_sql_cmd),0)=0 
then
  raise info ' ';
  raise info '******************************************** Not able to build SQL: Pivot Prep CMD ********************************************';
  return;
end if;

--Print Unpivot SQL
if v_include_sql=true
then
  select "ltrim" as "sqlparse" into v_sql_cmd_format from ltrim(v_sql_cmd);--Create a formatted verions of the query
  raise info ' ';
  raise info '/*** SQL: Pivot Prep CMD ***/';
  raise info '%', v_sql_cmd_format;
end if;
--Execute command
execute v_sql_cmd;

/*************************************************************************** Build SQL: Pivot with all diff CMD *************************************************************************************************/ 
--Build list of diff fields to create DIFF_ALL
select substring(listagg('+diff_' || col,'') within group(order by col) || ' as "diff_all"',2,10000) from tmp_cols where pk_flag=0 into v_ColList_diff;

drop table if exists tmp_compare;
select 'create temp table tmp_compare as select ' || v_ColList_diff || ',* from tmp_compare_prep;' into v_sql_cmd;

--If we have no CMD, abort
if isnull(len(v_sql_cmd),0)=0 
then
  raise info ' ';
  raise info '******************************************** Not able to build SQL: Pivot with all diff CMD ********************************************';
  return;
end if;

--Print Unpivot SQL
if v_include_sql=true
then
  select "ltrim" as "sqlparse" into v_sql_cmd_format from ltrim(v_sql_cmd);--Create a formatted verions of the query
  raise info ' ';
  raise info '/*** SQL: Pivot with all diff CMD ***/';
  raise info '%', v_sql_cmd_format;
end if;
--Execute command
execute v_sql_cmd;

/*************************************************************************** Build SQL: Union CMD (Un-Pivot) ************************************************************************************************************/ 
--set union string
update tmp_cols set buildstringunion = 'select ' || v_PKFields || ',''' || col || ''' as "col", cast(t1_' || col || ' as varchar(max)) as "t1" , cast(t2_' || col || ' as varchar(max)) as "t2" , diff_' || col || ' as "diff" from tmp_compare_prep union' 
where pk_flag=0
and rid <> (select max(rid) from tmp_cols where pk_flag=0);
--no union for last line
update tmp_cols set buildstringunion = 'select ' || v_PKFields || ',''' || col || ''' as "col", cast(t1_' || col || ' as varchar(max)) as "t1" , cast(t2_' || col || ' as varchar(max)) as "t2" , diff_' || col || ' as "diff" from tmp_compare_prep'
where pk_flag=0
and rid = (select max(rid) from tmp_cols where pk_flag=0);

--Build Cmd
drop table if exists tmp_compare_unpivot;
select listagg(buildstringunion,' ') within group(order by pk_flag desc, rid) into v_union_cmd from tmp_cols;
select 'create temp table tmp_compare_unpivot as ' || v_union_cmd into v_sql_cmd;

--If we have no CMD, abort
if isnull(len(v_sql_cmd),0)=0 
then
  raise info ' ';
  raise info '******************************************** Not able to build SQL: Union CMD (Un-Pivot) ********************************************';
  return;
end if;

--Print Un-Pivot SQL
if v_include_sql=true
then
  select "ltrim" as "sqlparse" into v_sql_cmd_format from ltrim(v_sql_cmd);--Create a formatted verions of the query
  raise info ' ';
  raise info '/*** SQL: Union CMD (Un-Pivot) ***/';
  raise info '%', v_sql_cmd_format;
end if;
--Execute Command
execute v_sql_cmd;

/******************************************************************************** Build SQL Left Join CMD **********************************************************************************************************/
--------------------------------------------------------------------------------------------------------------------------------------------------------
--Build v_from_cmd
--------------------------------------------------------------------------------------------------------------------------------------------------------
select replace(v_from_cmd,'inner join','left join') || isnull(' and ' || v_FilterCondition2,'') into v_from_outer_cmd;

--------------------------------------------------------------------------------------------------------------------------------------------------------
--Build v_where_cmd:
--Take any of the PK fields and check if it is null
--Add to v_where_cmd (parameter filter)
--------------------------------------------------------------------------------------------------------------------------------------------------------
select top 1 ' where t2.' || col || ' is null' || isnull(' and ' || v_FilterCondition1,'') from tmp_cols where pk_flag=1 order by rid  into v_where_cmd;

--------------------------------------------------------------------------------------------------------------------------------------------------------
--Build outer_col_list
--------------------------------------------------------------------------------------------------------------------------------------------------------
select listagg('t1.' || col,',') within group(order by pk_flag desc, rid) from tmp_cols into v_outer_col_list;

--------------------------------------------------------------------------------------------------------------------------------------------------------
--Build v_sql_cmd
--------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists tmp_missing_t2;
select 'create temp table tmp_missing_t2 as select ' || v_outer_col_list || ' ' || v_from_outer_cmd || isnull(v_where_cmd,'') into v_sql_cmd;
 
--If we have no CMD, abort
if isnull(len(v_sql_cmd),0)=0 
then
  raise info ' ';
  raise info '******************************************** Not able to build SQL Left Join CMD ********************************************';
  return;
end if;

if v_include_sql=true
then
  select "ltrim" as "sqlparse" into v_sql_cmd_format from ltrim(v_sql_cmd);--Create a formatted verions of the query
  raise info ' ';
  raise info '/*** SQL Left Join CMD ***/';
  raise info '%', v_sql_cmd_format;
end if;
--Execute Command
execute v_sql_cmd;

/*********************************************************************************** Build SQL Right Join CMD ************************************************************************************************************/

--------------------------------------------------------------------------------------------------------------------------------------------------------
--Switch to right join
--------------------------------------------------------------------------------------------------------------------------------------------------------
select replace(v_from_cmd,'inner join','right join') || isnull(' and ' || v_FilterCondition1,'') into v_from_outer_cmd;

--------------------------------------------------------------------------------------------------------------------------------------------------------
--Build v_where_cmd:
--Take any of the PK fields and check if it is null
--Add to v_where_cmd (parameter filter)
--------------------------------------------------------------------------------------------------------------------------------------------------------
select top 1 ' where t1.' || col || ' is null' || isnull(' and ' || v_FilterCondition2,'') from tmp_cols where pk_flag=1 order by rid  into v_where_cmd;

--------------------------------------------------------------------------------------------------------------------------------------------------------
--Build outer_col_list
--------------------------------------------------------------------------------------------------------------------------------------------------------
select listagg('t2.' || col,',') within group(order by pk_flag desc, rid) from tmp_cols into v_outer_col_list;

--------------------------------------------------------------------------------------------------------------------------------------------------------
--Build v_sql_cmd
--------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists tmp_missing_t1;
select 'create temp table tmp_missing_t1 as select ' || v_outer_col_list || ' ' || v_from_outer_cmd || isnull(v_where_cmd,'') into v_sql_cmd;

--If we have no CMD, abort
if isnull(len(v_sql_cmd),0)=0 
then
  raise info ' ';
  raise info '******************************************** Not able to build SQL Right Join CMD ********************************************';
  return;
end if;


if v_include_sql=true
then
  select "ltrim" as "sqlparse" into v_sql_cmd_format from ltrim(v_sql_cmd);--Create a formatted verions of the query
  raise info ' ';
  raise info '/*** SQL Right Join CMD ***/';
  raise info '%', v_sql_cmd_format;
end if;
--Execute Command
execute v_sql_cmd;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Print select statements
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
raise info ' ';
raise info '----SELECT for Pivotted output----';
raise info 'select top 100 * from tmp_compare where diff_all>.001 order by 1 desc;';

raise info ' ';
raise info '----SELECT for Un-Pivotted output: column with most differences ----';
raise info 'select col,count(1) as "differences" from tmp_compare_unpivot where diff<>0 group by col order by 2 desc;';
raise info ' ';
raise info '----SELECT for Un-Pivotted output:----';
raise info 'select top 100 * from tmp_compare_unpivot where diff<>0 order by diff desc;';

raise info ' ';
raise info '----SELECT for Missing T2:----';
raise info 'select top 100 * from tmp_missing_t2;';
raise info ' ';
raise info '----SELECT for Missing T1:----';
raise info 'select top 100 * from tmp_missing_t1;';

end;
$$ language plpgsql
;

