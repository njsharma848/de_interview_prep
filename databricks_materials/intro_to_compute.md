# Databricks Compute Resources

## Introduction to Compute in Databricks

In Databricks, **compute** essentially refers to a cluster of virtual machines. The terms **compute** and **cluster** are used interchangeably throughout the platform, so don't be confused when you hear either term - they refer to the same thing.

### Cluster Architecture

A typical Databricks cluster consists of:

- **Driver Node**: Orchestrates tasks and coordinates work across the cluster
- **Worker Nodes**: One or more nodes that perform the actual data processing

Together, these nodes enable Databricks clusters to run various workloads including:
- ETL (Extract, Transform, Load) operations
- Data science tasks
- Machine learning applications

---

## Types of Compute in Databricks

As we saw in the architecture diagram, Databricks offers two primary types of compute:

1. **Serverless Compute**: Available on-demand and fully managed by Databricks
2. **Classic Compute**: Configured and provisioned by the user

---

## Serverless Compute

Serverless compute is a fully managed service where Databricks handles all aspects of provisioning and management.

### Key Characteristics

**Resource Management:**
- Compute resources are provisioned in the **Databricks Cloud account**
- Databricks maintains a pool of pre-allocated virtual machines
- Clusters start immediately with minimal wait time

**Configuration & Scaling:**
- Databricks automatically configures clusters with the **latest runtime version**
- AI-powered intelligence handles cluster scaling (up and down)
- Automatic resource release when tasks complete

**Billing:**
- Customers are only charged for the duration the cluster is actively running
- No charges for idle time after task completion

### Benefits of Serverless Compute

✅ **Reduced Cluster Start Time**
- Instant availability from pre-allocated VM pool
- No waiting for fresh VM provisioning

✅ **Increased Productivity**
- Developers can focus on work, not infrastructure
- Automatic configuration reduces setup time

✅ **Lower Expected Costs**
- Reduced idle time
- AI-driven auto-scaling optimizes resource usage
- Pay only for active compute time

✅ **Reduced Administrative Burden**
- No cluster configuration required
- No maintenance overhead
- Databricks handles all management tasks

### Limitations (Preview Feature)

⚠️ **Current Status**: Mostly a preview feature with some limitations:

- **No Scala support** in notebooks
- **Lack of billing attribution** to workloads using tags
- **Limited feature set** compared to classic compute
- May not be fully available in all regions

**Exam Note**: Since serverless compute is still a preview feature, you may not encounter many exam questions about it. However, understanding the concept is valuable.

---

## Classic Compute

Classic compute provides complete control to the customer for cluster configuration and management.

### Key Characteristics

**Full Control Over:**
- Software version selection
- Compute node types
- Number of nodes
- Node size and configuration
- Cluster lifecycle management

**Management:**
- Customer-configured and customer-managed
- Resources provisioned in customer's cloud subscription (Managed Resource Group)
- Complete flexibility in cluster setup

---

## Types of Classic Compute

Databricks offers two major types of classic compute clusters:

### 1. All-Purpose Clusters

**Creation Method:**
- Created manually through:
  - Graphical User Interface (GUI)
  - Command Line Interface (CLI)
  - REST API

**Lifecycle:**
- **Persistent**: Can be terminated and restarted at any time
- Survives between different tasks and sessions
- Remains available for reuse

**Use Cases:**
- Interactive analysis
- Ad hoc analytical workloads
- Exploratory data analysis
- Development and testing

**Sharing:**
- Can be shared among multiple users
- Ideal for collaborative analysis
- Team-based data exploration

**Cost:**
- More expensive compared to job clusters
- Charges apply even during idle periods if not terminated

---

### 2. Job Clusters

**Creation Method:**
- Automatically created when an automated job starts
- Job must be configured to use a job cluster
- No manual creation required

**Lifecycle:**
- **Ephemeral**: Terminated automatically at the end of the job
- Cannot be restarted after termination
- Single-use only

**Use Cases:**
- Automated workloads
- Scheduled ETL pipelines
- Recurring machine learning tasks
- Production batch processing

**Isolation:**
- Dedicated to a single job execution
- No sharing with other users or jobs
- Complete resource isolation

**Cost:**
- Less expensive than all-purpose clusters
- Charges only for job execution duration
- No idle time costs

---

## All-Purpose vs Job Clusters: Detailed Comparison

| Aspect | All-Purpose Cluster | Job Cluster |
|--------|---------------------|-------------|
| **Creation** | Manual (GUI/CLI/API) | Automatic (job-triggered) |
| **Lifecycle** | Persistent | Ephemeral |
| **Restart Capability** | Can be terminated and restarted | Cannot be restarted after termination |
| **Persistence** | Remains available between sessions | Terminated after job completion |
| **Use Case** | Interactive & ad hoc analysis | Automated & scheduled workloads |
| **Ideal For** | Development, exploration, collaboration | Production ETL, recurring ML tasks |
| **Sharing** | Multi-user (collaborative) | Single-job (isolated) |
| **Cost** | Higher (includes idle time) | Lower (execution time only) |
| **Best Practices** | Development and interactive work | Production batch processing |

---

## When to Use Each Cluster Type

### Choose All-Purpose Clusters When:

✅ You need interactive data exploration  
✅ Multiple team members need to collaborate  
✅ You're doing development and testing  
✅ You need to quickly switch between different tasks  
✅ You want persistent compute available on-demand  

### Choose Job Clusters When:

✅ Running scheduled, automated jobs  
✅ Executing production ETL pipelines  
✅ Running recurring machine learning workloads  
✅ You want to minimize costs  
✅ You need resource isolation for specific tasks  
✅ Jobs have predictable start and end times  

---

## Summary

**Compute Options in Databricks:**

1. **Serverless Compute** (Preview)
   - Fully managed by Databricks
   - Fastest startup time
   - AI-powered scaling
   - Lower administrative overhead
   - Limited feature set (preview status)

2. **Classic Compute - All-Purpose**
   - Customer-managed
   - Persistent and shareable
   - Best for interactive work
   - Higher cost but more flexible

3. **Classic Compute - Job**
   - Customer-managed
   - Ephemeral and isolated
   - Best for automated workloads
   - Lower cost, optimized for production

**Key Takeaway**: All-purpose clusters are great for **interactive analysis and ad hoc work**, whereas job clusters are ideal for **repeated production workloads**.

---

Understanding these compute options helps you choose the right cluster type for your workload, optimize costs, and design efficient data engineering solutions in Databricks.