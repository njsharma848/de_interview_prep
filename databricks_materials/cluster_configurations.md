# Databricks Cluster Configuration Options

## Introduction

Classic compute in Databricks allows you to configure clusters yourself, providing flexibility and control. However, this flexibility comes with complexity - Databricks presents a variety of configuration options that need to be understood to optimize your cluster setup.

This guide walks through each cluster configuration option in detail to help you make informed decisions.

---

## Single Node vs Multi-Node Clusters

The first fundamental decision when creating a cluster is choosing between single node and multi-node architecture.

### Multi-Node Clusters

**Architecture:**
- **One driver node** + **one or more worker nodes**

**How It Works:**
- Driver node distributes tasks to worker nodes
- Enables parallel execution across multiple nodes
- Provides horizontal scalability based on workload demands

**Best For:**
- Large-scale workloads
- Distributed data processing
- Production ETL pipelines
- Workloads requiring horizontal scaling

**Scalability:**
- ✅ Can be horizontally scaled by adding more worker nodes
- ✅ Handles increasing data volumes effectively

---

### Single Node Clusters

**Architecture:**
- **Only a driver node** (no worker nodes)
- The same node acts as both master and worker

**How It Works:**
- Still supports Spark workloads
- All processing happens on a single machine
- No distribution of tasks across multiple nodes

**Limitations:**
- ❌ Cannot be horizontally scaled
- ❌ Unsuitable for large-scale ETL workloads
- ❌ Incompatible with process isolation
- ❌ Not intended for shared usage among multiple users

**Best For:**
- Lightweight machine learning tasks
- Data analysis that doesn't require distributed computing
- Development and testing
- Small datasets that fit on a single machine

**Databricks Recommendation:**
> When shared compute is needed, always use **multi-node clusters** to avoid conflicts and ensure proper resource isolation.

---

## Cluster Comparison

| Aspect | Multi-Node Cluster | Single Node Cluster |
|--------|-------------------|---------------------|
| **Architecture** | Driver + Worker nodes | Driver only |
| **Scalability** | Horizontal (add nodes) | None |
| **Distribution** | Tasks distributed across nodes | All tasks on one node |
| **Best For** | Large-scale production workloads | Lightweight ML & analysis |
| **Sharing** | Supports multiple users (with proper access mode) | Single user only |
| **Process Isolation** | Supported | Not supported |
| **Cost** | Higher (multiple VMs) | Lower (single VM) |

---

## Access Modes

Access mode is a critical security feature that controls **who can use the compute** and **what data they can access**.

Databricks offers three types of access modes:

### 1. Single User Access Mode

**Characteristics:**
- Allows only **one user** to access the cluster
- Most permissive in terms of capabilities

**Language Support:**
- ✅ Python
- ✅ SQL
- ✅ Scala
- ✅ R

**Use Cases:**
- Personal development and testing
- Individual data analysis
- When you need full language support

---

### 2. Shared Access Mode (Recommended for Production)

**Characteristics:**
- Enables **cluster sharing among multiple users**
- Provides **process isolation**
- Each process operates in its own environment
- Prevents one process from accessing another's data or credentials
- Includes **task preemption** for better resource management

**Availability:**
- Only available on **Premium workspaces**

**Language Support:**
- ✅ Python
- ✅ SQL
- ✅ Scala (Recently added with conditions)

**Scala Support Requirements:**
- Only available in Unity Catalog-enabled workspaces
- Requires Databricks Runtime 13.3 or higher

**Benefits:**
- 🔒 Enhanced security through process isolation
- 🔄 Task preemption prevents resource monopolization
- 👥 Safe multi-user collaboration

**Databricks Recommendation:**
> For production workloads, use **Shared Access Mode** as it offers process isolation and task preemption, making it more secure and reliable.

---

### 3. No Isolation Shared Mode

**Characteristics:**
- Allows cluster sharing among multiple users
- **No process isolation**
- **No task preemption**
- Failures or resource overuse by one user can affect others

**Availability:**
- Available on both **Standard** and **Premium** workspaces

**Language Support:**
- ✅ Python
- ✅ SQL
- ✅ Scala
- ✅ R

**Risks:**
- ⚠️ Security concerns due to lack of isolation
- ⚠️ One user's failures can impact others
- ⚠️ Resource contention issues
- ⚠️ Not recommended for production

**When to Use:**
- Development environments only
- Trusted user groups
- Non-production workloads

---

## Access Mode Comparison

| Feature | Single User | Shared (with Isolation) | No Isolation Shared |
|---------|-------------|------------------------|---------------------|
| **Users** | Single user only | Multiple users | Multiple users |
| **Process Isolation** | N/A | ✅ Yes | ❌ No |
| **Task Preemption** | N/A | ✅ Yes | ❌ No |
| **Workspace Tier** | Standard/Premium | Premium only | Standard/Premium |
| **Python Support** | ✅ | ✅ | ✅ |
| **SQL Support** | ✅ | ✅ | ✅ |
| **Scala Support** | ✅ | ✅ (Runtime 13.3+, Unity Catalog) | ✅ |
| **R Support** | ✅ | ❌ | ✅ |
| **Production Use** | Development only | ✅ Recommended | ❌ Not recommended |
| **Security Level** | High (single user) | Highest (isolation) | Low (no isolation) |

---

## Databricks Runtime

Databricks Runtime refers to the set of core libraries that run on Databricks clusters.

### Types of Databricks Runtime

At the time of recording, Databricks offers two main types of runtimes:

#### 1. Databricks Runtime

**What's Included:**
- Optimized version of **Apache Spark**
- Various supporting libraries
- Performance enhancements and bug fixes

**Best For:**
- General data engineering workloads
- ETL pipelines
- SQL analytics
- Standard Spark applications

---

#### 2. Databricks Runtime ML

**What's Included:**
- Everything in standard Databricks Runtime
- **Plus** popular machine learning libraries:
  - PyTorch
  - Keras
  - TensorFlow
  - XGBoost
  - scikit-learn
  - And more

**Best For:**
- Machine learning model training
- Deep learning workloads
- ML experimentation and development
- Data science projects

---

### Photon Acceleration

Both runtimes allow you to **enable or disable Photon**.

**What is Photon?**
- A vectorized query engine
- Accelerates Apache Spark workloads
- Provides significant performance improvements for SQL queries

**Benefits:**
- ⚡ Faster query execution
- 📊 Better performance for SQL analytics
- 💰 Potential cost savings through reduced execution time

**When to Enable:**
- SQL-heavy workloads
- Complex analytical queries
- Performance-critical applications

---

## Auto-Termination

Auto-termination is a cost-saving feature that automatically terminates idle clusters.

### How It Works

**Purpose:**
- Prevents unnecessary costs from idle clusters
- Automatically shuts down clusters after a period of inactivity

**Configuration:**
- **Default**: 120 minutes (2 hours)
- **Range**: 10 to 43,200 minutes (30 days)
- Fully customizable based on your needs

### Best Practices

**Set Short Timeouts For:**
- Development clusters (20-60 minutes)
- Personal compute (10-30 minutes)
- Interactive analysis clusters

**Set Longer Timeouts For:**
- Shared team clusters
- Clusters with long-running notebooks
- Production-like environments

**Example:**
```
Development Cluster: 20 minutes
Shared Team Cluster: 120 minutes
Production Job Cluster: Not applicable (terminates after job)
```

---

## Auto-Scaling

Auto-scaling automatically adjusts the number of nodes in a cluster based on workload demands.

### How Auto-Scaling Works

**Configuration:**
- Specify **minimum number of nodes**
- Specify **maximum number of nodes**
- Databricks automatically scales within this range

**Benefits:**
- 📊 Optimizes cluster utilization
- 💰 Reduces costs during low-demand periods
- ⚡ Provides additional resources during peak demand
- 🔄 Dynamic adjustment based on actual workload

### When to Use Auto-Scaling

**Ideal For:**
- ✅ Unpredictable workloads
- ✅ Fluctuating processing demands
- ✅ Variable data volumes
- ✅ Cost optimization

**Not Recommended For:**
- ❌ Steady, predictable workloads (use fixed size)
- ❌ Workloads requiring consistent performance
- ❌ When you need guaranteed resources

**Example Configuration:**
```
Minimum Nodes: 2 workers
Maximum Nodes: 8 workers

During low activity: Cluster scales down to 2 workers
During peak processing: Cluster scales up to 8 workers
```

---

## Spot Instances

Spot instances allow you to reduce costs by using spare cloud capacity at discounted prices.

### Key Characteristics

**What Are Spot Instances?**
- Unused VMs or spare capacity in the cloud
- Offered at significantly cheaper prices (up to 90% discount)
- Can be evicted if another customer acquires the instance at regular price

**Important Restrictions:**
- ✅ Can be used for **worker nodes only**
- ❌ **Driver nodes are always on-demand** (cannot use spot)

### How Databricks Handles Spot Instances

**Failover Mechanism:**
When spot instances become unavailable:
1. Databricks attempts to acquire replacement spot instances
2. If spot instances remain unavailable, switches to on-demand instances as fallback
3. Ensures job continuity

### When to Use Spot Instances

**Recommended For:**
- ✅ Fault-tolerant workloads
- ✅ Batch processing jobs
- ✅ Cost-sensitive applications
- ✅ Development and testing

**Not Recommended For:**
- ❌ Time-critical production workloads
- ❌ Real-time streaming applications
- ❌ Jobs that cannot tolerate interruptions
- ❌ Workloads requiring guaranteed completion times

---

## Cluster VM Types

Azure offers a range of VM types, which Databricks groups into specific categories optimized for different workload characteristics.

### 1. Memory Optimized

**Characteristics:**
- High memory-to-CPU ratio
- Large RAM capacity

**Best For:**
- 💾 Memory-intensive workloads
- 🤖 Machine learning workloads that cache large datasets
- 📊 In-memory analytics
- Large-scale data transformations requiring extensive caching

**Example Use Cases:**
- Training ML models with large datasets
- Graph processing
- Real-time analytics with large state

---

### 2. Compute Optimized

**Characteristics:**
- High CPU-to-memory ratio
- Powerful processing capabilities

**Best For:**
- ⚡ Structured streaming applications where peak processing rates are critical
- 📈 Distributed analytics
- 🔬 Data science workloads with heavy computations
- Complex transformations

**Example Use Cases:**
- Real-time event processing
- High-frequency data ingestion
- Complex analytical queries

---

### 3. Storage Optimized

**Characteristics:**
- High disk throughput
- Optimized I/O performance
- Large local storage capacity

**Best For:**
- 💿 Use cases requiring high disk throughput
- 📁 I/O-intensive operations
- Large file processing
- Log processing

**Example Use Cases:**
- Processing large files
- Disk-based sorting operations
- Heavy shuffle operations

---

### 4. General Purpose

**Characteristics:**
- Balanced compute, memory, and networking
- Versatile for various workloads

**Best For:**
- 🏢 Enterprise-grade applications
- 📊 Analytical workloads with in-memory caching
- Mixed workload requirements
- Standard data engineering tasks

**Example Use Cases:**
- General ETL pipelines
- Standard data transformations
- Typical analytical workloads

---

### 5. GPU Accelerated

**Characteristics:**
- Graphics Processing Units (GPUs)
- Massive parallel processing capability

**Best For:**
- 🧠 Deep learning models
- Data and compute-intensive tasks
- Neural network training
- Image and video processing

**Example Use Cases:**
- Training deep learning models
- Computer vision applications
- Natural language processing
- Large-scale ML model training

---

## VM Type Selection Guide

| VM Type | CPU | Memory | Storage | GPU | Best Workload |
|---------|-----|--------|---------|-----|---------------|
| **Memory Optimized** | Standard | High | Standard | No | ML with large cached datasets |
| **Compute Optimized** | High | Standard | Standard | No | Streaming, analytics |
| **Storage Optimized** | Standard | Standard | High | No | High I/O, large file processing |
| **General Purpose** | Balanced | Balanced | Balanced | No | Standard ETL, mixed workloads |
| **GPU Accelerated** | Standard | High | Standard | Yes | Deep learning, model training |

---

## Cluster Policies

With so many configuration options, creating clusters can become overwhelming for engineers and may result in oversized clusters that exceed budget constraints.

### What Are Cluster Policies?

Cluster Policies allow **administrators to set restrictions** and assign them to specific users or groups, simplifying cluster creation and controlling costs.

### How Cluster Policies Work

**Administrator Actions:**
1. Define cluster configuration constraints
2. Set default values for various options
3. Limit available choices (VM types, sizes, etc.)
4. Assign policies to users or groups

**User Experience:**
- Simplified user interface with limited options
- Pre-configured defaults reduce decision-making
- Prevents creation of oversized clusters
- Ensures compliance with organizational standards

### Example: Personal Compute Policy

When a **Personal Compute Policy** is applied, it might enforce:

```
Restrictions Applied:
├─ Cluster Type: Single node only
├─ Runtime: ML runtime (default)
├─ Node Types: Limited selection
├─ Auto-Termination: 20 minutes (enforced)
├─ Spot Instances: Not allowed
└─ Max Workers: N/A (single node)
```

### Benefits of Cluster Policies

**For Administrators:**
- 🎯 Control costs by limiting cluster sizes
- 🔒 Enforce security and compliance standards
- 📋 Reduce support burden with standardized configs
- 📊 Better budget predictability

**For Users:**
- ✅ Simplified cluster creation interface
- ⚡ Faster decision-making with fewer options
- 📚 Guided configuration with appropriate defaults
- ❌ Reduced risk of misconfiguration

**For Organizations:**
- 💰 Cost control and budget compliance
- 🔄 Consistent cluster configurations
- 🛡️ Security policy enforcement
- 📈 Better resource utilization

---

## Configuration Best Practices Summary

### Development Environments
```
Cluster Type: Single node or small multi-node (2-4 workers)
Access Mode: Single user
Runtime: Standard Databricks Runtime
Auto-Termination: 20-30 minutes
VM Type: General purpose
Spot Instances: Yes (for workers)
```

### Production ETL Workloads
```
Cluster Type: Multi-node with auto-scaling
Access Mode: Shared (with isolation)
Runtime: Standard Databricks Runtime with Photon
Auto-Termination: Job-based (job clusters)
VM Type: Compute or storage optimized
Spot Instances: Carefully (based on SLA requirements)
```

### Machine Learning Workloads
```
Cluster Type: Multi-node
Access Mode: Single user or Shared
Runtime: Databricks Runtime ML
Auto-Termination: 60-120 minutes
VM Type: Memory optimized or GPU accelerated
Spot Instances: For training only (not for serving)
```

### Shared Team Analysis
```
Cluster Type: Multi-node with auto-scaling
Access Mode: Shared (with isolation)
Runtime: Standard Databricks Runtime
Auto-Termination: 120 minutes
VM Type: General purpose
Spot Instances: No (for reliability)
```

---

## Summary

Databricks provides extensive cluster configuration options to optimize for different workload requirements:

1. **Choose the right architecture**: Single node for lightweight tasks, multi-node for production
2. **Select appropriate access mode**: Use Shared mode with isolation for production multi-user scenarios
3. **Pick the right runtime**: Standard for ETL, ML runtime for machine learning
4. **Enable auto-termination**: Prevent unnecessary costs from idle clusters
5. **Configure auto-scaling**: Optimize resource usage for variable workloads
6. **Consider spot instances**: Reduce costs for fault-tolerant workloads
7. **Match VM types to workload**: Choose based on compute, memory, storage, or GPU requirements
8. **Implement cluster policies**: Simplify management and control costs

Understanding these configuration options enables you to create optimized, cost-effective clusters tailored to your specific workload requirements.

---

In the next lesson, we'll create a cluster using these configuration options and see them in action.