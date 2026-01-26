Databricks asked these advanced SQL + Delta questions in a Data Engineering interview (2+ YOE)
CTC: 20.9 LPADifficulty: Advanced
	1.	Write a Delta SQL query to compute cumulative revenue per day using window frames and handle late-arriving events.
	2.	Get the first and last login time per user from a huge events table (must be performant at scale).
	3.	Find top-3 customers by spend in each region using window functions and tie-breakers.
	4.	Detect duplicates in a transactional Delta table and safely deduplicate using MERGE (idempotent delete).
	5.	Return users who made purchases in three consecutive months (month-over-month streaks).
	6.	Identify skewed joins from EXPLAIN plans and propose fixes (broadcast, salting, repartition).
	7.	Compute a 7-day moving average of product sales on Delta Lake using event-time windows.
	8.	Pivot daily sales into month-wise columns (wide transform) and keep it streaming-friendly.
	9.	Find customers who bought at least once every month in a calendar year (set-based approach).
	10.	Rank products by yearly sales, reset ranking each year (window partition by year).
	11.	Find employees earning more than their department average (correlated subquery or window).
	12.	Calculate median transaction amount without a built-in MEDIAN (approx_percentile / percentile_cont approach).
	13.	Get all users who placed their first order in the last 30 days (first_value / row_number over partition).
	14.	Compare price change between two dates for each product (self-join or LAG with date filtering).
	15.	Identify customers whose first and last transaction occurred on the same day (lifecycle edge-case).
	16.	Calculate monthly percentage of returning users (cohort + month-over-month retention).
	17.	List products that have never been sold (anti-join / left anti).
	18.	Detect schema drift across historical Delta snapshots and propose remediation steps.
	19.	Find departments where at least two employees share the exact salary (group + having count >=2).
	20.	Group users by login streaks of 3+ consecutive days (gaps-and-islands pattern).​​​​​​​​​​​​​​​​