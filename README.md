# sp_compare_table

A stored procedure for comparing any two RedShift tables or views and showing their differences. This creates 4 temporary tables that highlight differences between the compared tables:

1. `tmp_compare` - Side-by-side comparison with a "diff" column
2. `tmp_compare_unpivot` - Unpivoted view showing differences by column
3. `tmp_missing_t2` - Records present in table 1 but missing from table 2
4. `tmp_missing_t1` - Records present in table 2 but missing from table 1

The unpivotted result (2) is useful when comparing wide datasets with many features, such as modeling datasets used for machine learning. 
For example, say we want to compare two different versions of this dataset:

| snapshot_date | product_key | product_status | product_cost | product_units_sold |
|--------------|-------------|----------------|--------------|-------------------|
| 2024-01-15   | PRD-A102    | active         | $125.99      | 847              |
| 2024-01-15   | PRD-B445    | discontinued   | $89.50       | 234              |
| 2024-01-15   | PRD-C789    | active         | $299.99      | 1256             |
| 2024-01-15   | PRD-D332    | pending        | $45.75       | 567              |
| 2024-01-15   | PRD-E901    | active         | $199.99      | 932              |

The stored procedure generates a temporary table in this format:

| snapshot_date | product_key | col               | t1        | t2        | diff    |
|--------------|-------------|-------------------|-----------|-----------|---------|
| 2024-01-15   | PRD-A102    | product_status    | active    | inactive  | 1       |
| 2024-01-15   | PRD-A102    | product_cost      | 125.99    | 130.99    | 3.97    |
| 2024-01-15   | PRD-A102    | product_units_sold| 847       | 912       | 7.67    |
| 2024-01-15   | PRD-B445    | product_status    | discontinued| discontinued| 0       |
| 2024-01-15   | PRD-B445    | product_cost      | 89.50     | 89.50     | 0.00    |
| 2024-01-15   | PRD-B445    | product_units_sold| 234       | 198       | -15.38  |
| 2024-01-15   | PRD-C789    | product_status    | active    | active    | 0       |
| 2024-01-15   | PRD-C789    | product_cost      | 299.99    | 279.99    | -6.67   |
| 2024-01-15   | PRD-C789    | product_units_sold| 1256      | 1489      | 18.55   |
| 2024-01-15   | PRD-D332    | product_status    | pending   | active    | 1       |
| 2024-01-15   | PRD-D332    | product_cost      | 45.75     | 49.99     | 9.27    |
| 2024-01-15   | PRD-D332    | product_units_sold| 567       | 634       | 11.82   |
| 2024-01-15   | PRD-E901    | product_status    | active    | active    | 0       |
| 2024-01-15   | PRD-E901    | product_cost      | 199.99    | 199.99    | 0.00    |
| 2024-01-15   | PRD-E901    | product_units_sold| 932       | 1045      | 12.12   |

This allows us to focus on the columns having the largest differences:
```sql
select col, count(1) as "diff_count"
from tmp_compare_unpivot
where diff<>0
group by col
order by 2 desc;
```

| col               | diff_count |
|------------------|------------|
| product_units_sold| 5          |
| product_cost      | 3          |
| product_status    | 2          |

Or we can group on the PK fields to show the records with most differences:
```sql
select snapshot_date, product_key, count(1) as "diff_count" 
from tmp_compare_unpivot 
where diff<>0 
group by snapshot_date, product_key 
order by 3 desc;
```

| snapshot_date | product_key | diff_count |
|--------------|-------------|------------|
| 2024-01-15   | PRD-A102    | 3          |
| 2024-01-15   | PRD-D332    | 3          |
| 2024-01-15   | PRD-C789    | 2          |
| 2024-01-15   | PRD-E901    | 1          |
| 2024-01-15   | PRD-B445    | 1          |

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

## Output

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
