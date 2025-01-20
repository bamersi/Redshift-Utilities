------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Create two tables to compare
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists operations.table_1
;
create table operations.table_1(pkfield int, snapshot_date date, char_field varchar(50), float_field float, decimal_field decimal(18,2))
;
insert into operations.table_1
values(1, '19720109', 'xxx', 111.111, 100.10)
,(2, '19720109', 'yyy', 222.222, 200.20)
,(3, '19840101', 'zzz', 333.333, 300.30)
,(99, '19720109', 'aaa', 333.333, 300.30)
;

drop table if exists operations.table_2
;
create table operations.table_2(pkfield int, snapshot_date date, char_field varchar(50), float_field float, decimal_field decimal(18,2))
;
insert into operations.table_2
values(1, '19720109', 'xxx', 111.111, 100.10)
,(2, '19720109', 'yyy', 222.223, 222.20)
,(3, '19840101', 'zzz', 333.333, 300.30)
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Compare tables
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
call operations.sp_compare_table   
(
'operations.table_1'                                     --v_SchemaTable1
,'operations.table_2'                                    --v_SchemaTable2
,'pkfield'                                               --v_PKFields
,null                                                    --v_CompareFields 
,null                                                    --v_ExcludeFields
,'t1.snapshot_date=''19720109'''                         --v_FilterCondition1                        
,'t2.snapshot_date=''19720109'''                         --v_FilterCondition2                        
,null                                                    --v_include_sql  
,2                                                       --v_scale
)
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Check Compare Output
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----SELECT for Pivotted output----
select top 100 * from tmp_compare where diff_all>.001 order by 1 desc;
 
----SELECT for Un-Pivotted output: column with most differences ----
select col,count(1) as "differences" from tmp_compare_unpivot where diff<>0 group by col order by 2 desc;
 
----SELECT for Un-Pivotted output:----
select top 100 * from tmp_compare_unpivot where diff<>0 order by diff desc;
 
----SELECT for Missing T2:----
select top 100 * from tmp_missing_t2;
 
----SELECT for Missing T1:----
select top 100 * from tmp_missing_t1;

