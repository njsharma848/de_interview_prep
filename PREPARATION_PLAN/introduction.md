Based on your DynamoDB statement, here are the types of questions you should be prepared to answer:

## **Partition Key & Schema Design (Core Claims)**

**Basic:**
- "Walk me through how you chose partition keys for your DynamoDB tables. What was your decision-making process?"
- "What makes a good partition key? What makes a bad one?"
- "Can you give me an example of a table schema you designed and explain why you structured it that way?"

**Intermediate:**
- "How did you handle hot partitions? Did you encounter any, and how did you detect and resolve them?"
- "Tell me about a time when you had to redesign a partition key. What was the problem and how did you migrate the data?"
- "How do you balance between normalizing data and denormalizing for DynamoDB?"

**Advanced:**
- "How would you design a DynamoDB table for a multi-tenant application while ensuring tenant isolation and preventing noisy neighbor issues?"
- "Explain the difference between partition key and sort key. When would you use a composite primary key?"

## **Global Secondary Indexes (GSI)**

**Basic:**
- "What's the difference between a GSI and an LSI?"
- "Why did you choose GSIs over LSIs in your designs?"
- "How many GSIs did you typically use per table?"

**Intermediate:**
- "GSIs have their own provisioned throughput. How did you determine the right capacity for them?"
- "What happens when a GSI gets throttled? How did you handle this?"
- "Can you explain how GSI projection works? What's the tradeoff between KEYS_ONLY, INCLUDE, and ALL projections?"

**Advanced:**
- "GSIs are eventually consistent. Did this cause any issues in your use cases? How did you handle it?"
- "How do GSIs impact write costs? Walk me through the cost implications of your GSI design."
- "Have you dealt with GSI backfill issues during table updates? How did you handle them?"

## **Access Patterns & Query Optimization**

**Basic:**
- "What access patterns were you supporting with your DynamoDB tables?"
- "How do you query DynamoDB? What's the difference between Query and Scan?"

**Intermediate:**
- "How did you achieve low latency? What was your P99 latency target and did you meet it?"
- "Walk me through how you would model a one-to-many relationship in DynamoDB."
- "Did you use any caching layer (DAX)? Why or why not?"

**Advanced:**
- "How do you handle transactions in DynamoDB? Did you use them, and what were the limitations?"
- "Explain how you would implement pagination for a large result set."
- "How would you design a table to support both point lookups and range queries efficiently?"

## **Capacity Planning & Auto Scaling**

**Basic:**
- "What's the difference between provisioned capacity and on-demand mode?"
- "How did you decide between RCU/WCU provisioned capacity vs on-demand?"

**Intermediate:**
- "Walk me through how you configured Auto Scaling for your tables. What were your target utilization thresholds?"
- "How quickly does DynamoDB Auto Scaling respond to traffic spikes? Did you face any issues with scaling delays?"
- "How did you calculate the required RCUs and WCUs for your workload?"

**Advanced:**
- "What metrics did you monitor to tune your capacity planning? How did you use CloudWatch for this?"
- "Have you dealt with throttling exceptions? How do you implement exponential backoff and jitter?"
- "Auto Scaling has limitations. What happens during sudden traffic spikes that exceed scaling speed?"

## **PySpark Integration & ETL**

**Basic:**
- "How did you read from DynamoDB in PySpark? What connector did you use?"
- "How did you write data from PySpark to DynamoDB?"

**Intermediate:**
- "What challenges did you face when integrating DynamoDB with Spark? How did you handle them?"
- "How did you handle batch writes to DynamoDB from Spark? Did you use BatchWriteItem?"
- "DynamoDB has a 25 write item limit for batch operations. How did you handle larger datasets in PySpark?"

**Advanced:**
- "How did you optimize parallel writes from Spark to DynamoDB to avoid throttling?"
- "Did you implement any retry logic or error handling for DynamoDB writes in your PySpark jobs?"
- "How did you handle failed writes? Did you implement a DLQ (Dead Letter Queue) pattern?"

## **Monitoring & Performance**

**Basic:**
- "What CloudWatch metrics did you monitor for DynamoDB?"
- "How did you detect performance issues in your DynamoDB tables?"

**Intermediate:**
- "What was your approach to monitoring consumed capacity vs provisioned capacity?"
- "How did you track throttling events? What thresholds triggered your alarms?"
- "Tell me about a performance issue you debugged in DynamoDB. What was the root cause?"

**Advanced:**
- "How do you optimize costs for DynamoDB while maintaining performance?"
- "What's your strategy for handling burst capacity? Can you rely on it?"

## **Behavioral/Scenario Questions**

- "Tell me about a time when you had to migrate data from one DynamoDB table to another with a different schema."
- "Describe a situation where your initial DynamoDB design didn't work well. How did you identify and fix it?"
- "How would you handle a requirement to add a new access pattern to an existing DynamoDB table?"

---

## **Red Flag Questions (Be Careful)**

These questions test if you truly understand DynamoDB vs just used it superficially:

- "Why DynamoDB over RDS/PostgreSQL for your use case?" (Need a good justification)
- "What were the consistency requirements? Did you ever need strong consistency reads?"
- "How did you handle joins or complex queries that DynamoDB doesn't natively support?"
- "What would you do differently if you designed those tables again today?"

---

**My Recommendation:**

You should be prepared to:
1. **Describe at least one specific table schema** you designed (table name, partition key, sort key, GSIs, access patterns)
2. **Know the actual RCU/WCU numbers** you provisioned and why
3. **Have a performance story** ready (latency improvements, cost optimizations, or problem-solving)
4. **Understand the PySpark-DynamoDB integration** at a code level (boto3, connector libraries)
5. **Be honest about limitations** - what problems did DynamoDB NOT solve well?

Would you like me to help you prepare detailed answers for any of these specific question categories?
