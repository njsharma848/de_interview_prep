**If your SQL only works on sample data, 𝘆𝗼𝘂 𝗱𝗼𝗻’𝘁 𝗸𝗻𝗼𝘄 SQL yet...**

**However, during my third year as a data engineer, one SQL query brought a pipeline to its knees**

**not because it failed, but because it was slow, unreadable, and unfixable under pressure. That experience changed my perspective on SQL.**

**In data engineering, SQL is not just about correctness; it’s about performance, intent, and survivability.**

**Here are some hard-earned insights that truly matter:**

1. **𝗪𝗿𝗶𝘁𝗲 𝗳𝗼𝗿 𝘁𝗵𝗲 𝗻𝗲𝘅𝘁 𝗲𝗻𝗴𝗶𝗻𝗲𝗲𝗿, not for ego. Use Common Table Expressions (CTEs) instead of nested subqueries, and prioritize clear names over clever tricks.**

2. **𝗔𝗹𝘄𝗮𝘆𝘀 𝗰𝗼𝗻𝘀𝗶𝗱𝗲𝗿 𝗱𝗮𝘁𝗮 𝘃𝗼𝗹𝘂𝗺𝗲. A query that works on 10K rows can fail with 100M. Question all joins, filters, and aggregations.**

3. **𝗜𝗻𝗱𝗲𝘅𝗲𝘀 𝗮𝗻𝗱 𝗽𝗮𝗿𝘁𝗶𝘁𝗶𝗼𝗻𝘀 are integral to SQL. If you don’t understand how your query is executed, you don’t fully grasp SQL yet.**

4. **𝗘𝘅𝗽𝗹𝗮𝗶𝗻 𝗽𝗹𝗮𝗻𝘀 𝗮𝗿𝗲 𝗲𝘀𝘀𝗲𝗻𝘁𝗶𝗮𝗹. If you’re not reviewing them, you’re merely guessing.**

**Quick tip: If a JOIN suddenly slows down, check for data skew, examine join order, and push filters before the join. Many SQL “bugs” are actually design problems.**
