# RedShift Table Comparison Tool

A powerful stored procedure for comparing any two RedShift tables or views and analyzing their differences.

## Overview

This tool creates 4 temporary tables that highlight differences between the source tables:

1. `tmp_compare` - Side-by-side comparison with a "diff" column
2. `tmp_compare_unpivot` - Unpivoted view showing differences by column
3. `tmp_missing_t2` - Records present in table 1 but missing from table 2
4. `tmp_missing_t1` - Records present in table 2 but missing from table 1

## Usage

After running the comparison, use these queries to analyze the results:

```sql
-- View records with differences (pivoted format)
select top 100 * from tmp_compare where diff_all>.001 order by 1 desc;

-- Summary of differences by column
select col,count(1) as "differences" 
from tmp_compare_unpivot 
where diff<>0 
group by col 
order by 2 desc;

-- Detailed differences by column (unpivoted)
select top 100 * from tmp_compare_unpivot where diff<>0 order by diff desc;

-- Records missing from table 2
select top 100 * from tmp_missing_t2;

-- Records missing from table 1
select top 100 * from tmp_missing_t1;
```

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| v_SchemaTable1 | Yes | First table/view to compare (internal table, spectrum table, or view) |
| v_SchemaTable2 | Yes | Second table/view to compare (internal table, spectrum table, or view) |
| v_PKFields | Yes | Comma-separated list of Primary Key columns |
| v_CompareFields | No | Columns to compare (use '*' or NULL for all non-PK fields) |
| v_Excludefields | No | Columns to exclude from comparison |
| v_FilterCondition1 | No | Filter for table 1 (e.g., "t1.column_name = 'value'") |
| v_FilterCondition2 | No | Filter for table 2 (e.g., "t2.column_name = 'value'") |
| v_include_sql | No | Include SQL statements in output (default: false) |
| v_scale | No | Round numerics to this scale before comparing (default: 100) |

## Example

```sql
call operations.sp_compare_table(
    'operations.table_1',                    -- v_SchemaTable1
    'operations.table_2',                    -- v_SchemaTable2
    'pkfield',                              -- v_PKFields
    null,                                   -- v_CompareFields
    'ignorefield',                          -- v_ExcludeFields
    't1.snapshot_date=''19720109''',        -- v_FilterCondition1
    't2.snapshot_date=''19720109''',        -- v_FilterCondition2
    true,                                   -- v_include_sql
    2                                       -- v_scale
);
```

## Installation

1. Run `DDL - 001 - sp_compare_table.sql` to create the stored procedure
   - By default, creates in "operations" schema
   - Modify script to use a different schema if needed

2. Verify installation by running:
```sql
call operations.sp_compare_table(null,null,null,null,null,null,null,null,null)
```

## Testing

Run `TEST - 001 - sp_compare_table.sql` to create and compare test tables.

## TODO

1. Add support for mixing internal and external tables
2. Add parameter to filter percent diff
3. Update v_scale logic:
   - Remove rounding on compare output
   - Cast rounded value to decimal
   - Maintain NULL v_scale behavior using float
4. Evaluate using EXCEPT for missing data comparison (performance testing needed)
