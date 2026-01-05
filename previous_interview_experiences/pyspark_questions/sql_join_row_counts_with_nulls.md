# SQL Joins Examples

This document compiles explanations of various SQL join types based on different table examples, including row counts and detailed resulting rows where applicable.

## Example 1: Tables with Matching `*` Values and NULLs

### Table Structure
```
Table A     Table B 
*             *
*             *
*             NULL
NULL          NULL
NULL          NULL
```

### Query
Could you please help me in understanding how many rows return for below join types?  
Inner Join: ?  
Left Join: ?  
Right Join: ?  
Full join: ?  

### Response
Inner Join: 6  
Left Join: 8  
Right Join: 9  
Full Join: 11  

To arrive at these numbers, assume both tables have a single column being joined on (with `*` representing the same non-NULL value across all such entries, as is typical in these examples). Table A has 3 matching non-NULL rows and 2 NULL rows. Table B has 2 matching non-NULL rows and 3 NULL rows.  

- **Inner Join**: Returns only rows where the join condition is met. The 3 non-NULL rows in A each match the 2 non-NULL rows in B (3 × 2 = 6). NULLs do not match anything (including other NULLs), so no additional rows. Total: 6.  
- **Left Join**: Returns all rows from A, plus matches from B where available (otherwise NULL for B's columns). The 6 matches from above, plus the 2 unmatched NULL rows from A. Total: 8.  
- **Right Join**: Returns all rows from B, plus matches from A where available (otherwise NULL for A's columns). The 6 matches from above, plus the 3 unmatched NULL rows from B. Total: 9.  
- **Full Join**: Returns all rows from both tables, with matches where available (otherwise NULLs for the missing side's columns). The 6 matches, plus the 2 unmatched from A, plus the 3 unmatched from B. Total: 11.  

## Example 2: Similar Tables with `*` Values and NULLs (Outer Joins Only)

### Table Structure
```
Table A Table B
*       *
*       *
*       NULL
NULL    NULL
NULL    NULL
```

### Query
Could you please help me in understanding how many rows return for below join types?  
Left Outer Join: ?  
Right Outer Join: ?  
Full Outer join: ?  

### Response
Left Outer Join: 8  
Right Outer Join: 9  
Full Outer Join: 11  

To arrive at these numbers, assume both tables have a single column being joined on (with `*` representing the same non-NULL value across all such entries, as is typical in these examples). Table A has 3 matching non-NULL rows and 2 NULL rows. Table B has 2 matching non-NULL rows and 3 NULL rows.  

- **Left Outer Join**: Returns all rows from A, plus matches from B where available (otherwise NULL for B's columns). The 3 non-NULL rows in A each match the 2 non-NULL rows in B (3 × 2 = 6), plus the 2 unmatched NULL rows from A. Total: 8.  
- **Right Outer Join**: Returns all rows from B, plus matches from A where available (otherwise NULL for A's columns). The 6 matches from above, plus the 3 unmatched NULL rows from B. Total: 9.  
- **Full Outer Join**: Returns all rows from both tables, with matches where available (otherwise NULLs for the missing side's columns). The 6 matches, plus the 2 unmatched from A, plus the 3 unmatched from B. Total: 11.  

## Example 3: Tables with Numeric Values and NULLs

### Table Structure
```
Table A Table B
1       2
2       1
3       NULL
NULL    NULL
NULL    NULL
```

### Query
Could you please help me in understanding how many rows return for below join types?  
Inner Join: ?  
Left Join: ?  
Right Join: ?  
Full join: ?  

### Response
Inner Join: 2  
Left Join: 5  
Right Join: 5  
Full Join: 8  

To arrive at these numbers, assume both tables have a single column being joined on. Table A has rows with values 1, 2, 3, NULL, NULL. Table B has rows with values 2, 1, NULL, NULL, NULL. NULL values do not match anything (including other NULLs). There are no duplicate non-NULL values within each table, so matches are one-to-one with no Cartesian product expansion.  

- **Inner Join**: Returns only rows where the join condition is met. The row with 1 in A matches the row with 1 in B (1 row). The row with 2 in A matches the row with 2 in B (1 row). No other matches. Total: 2.  
- **Left Join**: Returns all rows from A, plus matches from B where available (otherwise NULL for B's columns). The 2 matches from above, plus the unmatched row with 3 from A, plus the 2 unmatched NULL rows from A. Total: 5.  
- **Right Join**: Returns all rows from B, plus matches from A where available (otherwise NULL for A's columns). The 2 matches from above, plus the 3 unmatched NULL rows from B. Total: 5.  
- **Full Join**: Returns all rows from both tables, with matches where available (otherwise NULLs for the missing side's columns). The 2 matches, plus the 3 unmatched from A (3 and 2 NULLs), plus the 3 unmatched from B (3 NULLs). Total: 8.  

## Example 4: Tables with Duplicate Numeric Values and NULLs

### Table Structure
```
Table_1 Table_2
1       1
1       1
1       NULL
NULL    NULL
NULL    NULL
```

### Query
Could you please help me in understanding how many rows return for below join types?  
Inner Join: ?  
Left Join: ?  
Right Join: ?  
Full join: ?  

### Response
Inner Join: 6  
Left Join: 8  
Right Join: 9  
Full Join: 11  

To arrive at these numbers, assume both tables have a single column being joined on. Table_1 has rows with values 1, 1, 1, NULL, NULL. Table_2 has rows with values 1, 1, NULL, NULL, NULL. NULL values do not match anything (including other NULLs).  

- **Inner Join**: Returns only rows where the join condition is met. Each of the 3 rows with 1 in Table_1 matches each of the 2 rows with 1 in Table_2 (3 × 2 = 6). No other matches. Total: 6.  
- **Left Join**: Returns all rows from Table_1, plus matches from Table_2 where available (otherwise NULL for Table_2's columns). The 6 matches from above, plus the 2 unmatched NULL rows from Table_1. Total: 8.  
- **Right Join**: Returns all rows from Table_2, plus matches from Table_1 where available (otherwise NULL for Table_1's columns). The 6 matches from above, plus the 3 unmatched NULL rows from Table_2. Total: 9.  
- **Full Join**: Returns all rows from both tables, with matches where available (otherwise NULLs for the missing side's columns). The 6 matches, plus the 2 unmatched from Table_1, plus the 3 unmatched from Table_2. Total: 11.  

## Detailed Rows for Joins (Based on Example 4)

If we want to know which rows will be returned for each join?

### Inner Join
Returns only the rows where the join condition is met (non-NULL values that match). Since Table_1 has three rows with value 1 and Table_2 has two rows with value 1, this produces a Cartesian product of matches: 3 × 2 = 6 rows. NULLs do not match anything, including other NULLs.  

The resulting rows (as pairs: (Table_1.value, Table_2.value)) are:  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  

To arrive at this: Identify matching non-NULL values. Each 1 in Table_1 pairs with each 1 in Table_2, resulting in 6 identical pairs.  

### Left Join
Returns all rows from Table_1, with matching rows from Table_2 where available (otherwise NULL for Table_2's column). This includes the 6 matching rows from the inner join, plus the 2 unmatched NULL rows from Table_1 (with NULL for Table_2).  

The resulting rows (as pairs: (Table_1.value, Table_2.value)) are:  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (NULL, NULL)  
- (NULL, NULL)  

To arrive at this: Start with all 5 rows from Table_1. For the three 1's, each matches both 1's in Table_2 (6 rows). For the two NULLs in Table_1, no matches, so pad with NULL on the right. Total: 8 rows.  

### Right Join
Returns all rows from Table_2, with matching rows from Table_1 where available (otherwise NULL for Table_1's column). This includes the 6 matching rows from the inner join, plus the 3 unmatched NULL rows from Table_2 (with NULL for Table_1).  

The resulting rows (as pairs: (Table_1.value, Table_2.value)) are:  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (NULL, NULL)  
- (NULL, NULL)  
- (NULL, NULL)  

To arrive at this: Start with all 5 rows from Table_2. For the two 1's, each matches all three 1's in Table_1 (6 rows). For the three NULLs in Table_2, no matches, so pad with NULL on the left. Total: 9 rows.  

### Full Join
Returns all rows from both tables, with matches where available (otherwise NULLs for the missing side's column). This includes the 6 matching rows from the inner join, plus the 2 unmatched NULL rows from Table_1 (with NULL for Table_2), plus the 3 unmatched NULL rows from Table_2 (with NULL for Table_1).  

The resulting rows (as pairs: (Table_1.value, Table_2.value)) are:  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (1, 1)  
- (NULL, NULL)  
- (NULL, NULL)  
- (NULL, NULL)  
- (NULL, NULL)  
- (NULL, NULL)  

To arrive at this: Combine the left join (8 rows) and right join (9 rows), but subtract the overlapping inner join (6 rows) to avoid double-counting matches: 8 + 9 - 6 = 11 rows. The extra NULL pairs come from the unmatched sides.
