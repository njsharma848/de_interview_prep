WITH RECURSIVE EmployeeHierarchy AS (
    -- Base case
    SELECT id, employee_name, manager_id, 0 AS level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case
    SELECT e.id, e.employee_name, e.manager_id, eh.level + 1
    FROM employees e
    INNER JOIN EmployeeHierarchy eh ON e.manager_id = eh.id
)
SELECT * FROM EmployeeHierarchy
ORDER BY level, id;
```

**Result:**
```
id | employee_name | manager_id | level
---|---------------|------------|-------
1  | Alice         | NULL       | 0
2  | Bob           | 1          | 1
3  | Charlie       | 1          | 1
4  | Diana         | 2          | 2
5  | Eve           | 2          | 2
6  | Frank         | 3          | 2