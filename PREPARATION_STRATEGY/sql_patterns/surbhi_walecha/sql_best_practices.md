# SQL Best Practices

✔ Use `EXISTS` in place of `IN` wherever possible

✔ Use table aliases with columns when you are joining multiple tables

✔ Use `GROUP BY` instead of `DISTINCT`

✔ Add useful comments wherever you write complex logic and avoid too many comments

✔ Use joins instead of subqueries when possible for better performance

✔ Use `WHERE` instead of `HAVING` to define filters on non-aggregate fields

✔ Avoid wildcards at beginning of predicates (something like `'%abc'` will cause full table scan to get the results)

✔ Considering cardinality within `GROUP BY` can make it faster (try to consider unique column first in group by list)

✔ Write SQL keywords in capital letters

✔ Never use `SELECT *`, always mention list of columns in select clause

✔ Create CTEs instead of multiple subqueries, it will make your query easy to read

✔ Join tables using `JOIN` keywords instead of writing join condition in where clause for better readability

✔ Never use `ORDER BY` in subqueries, it will unnecessarily increase runtime

✔ If you know there are no duplicates in 2 tables, use `UNION ALL` instead of `UNION` for better performance

✔ Always start `WHERE` clause with `1 = 1`. This has the advantage of easily commenting out conditions during debugging a query

✔ Taking care of NULL values before using equality or comparisons operators. Applying window functions. Filtering the query before joining and having clause

✔ Make sure the JOIN conditions among two tables are either keys or indexed attributes