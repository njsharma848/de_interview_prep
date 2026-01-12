# Databricks Architecture Overview

The Databricks architecture is divided into two main parts: the **Control Plane** and the **Compute Plane**.

---

## Control Plane

The Control Plane handles all the backend services required for the Databricks platform. It includes several critical components:

### Key Components

#### 1. Databricks Web User Interface
- Browser-based interface for user interaction
- Provides access to all Databricks functionality
- Central hub for managing notebooks, clusters, and jobs

#### 2. Cluster Manager
- Responsible for managing and provisioning compute resources
- Handles cluster creation and lifecycle management
- Manages cluster scaling (up and down) based on user requirements

#### 3. Unity Catalog
- Provides comprehensive data governance
- Manages access control and permissions for your data
- Centralized metadata management across workspaces

#### 4. Storage for Queries and Workspace Data
- Stores workspace-related metadata
- Contains information such as:
  - Notebook revisions
  - Job run details
  - Query history
  - Workspace configurations

---

## Compute Plane

The Compute Plane is where your actual data processing takes place. Databricks supports two distinct types of compute models:

### 1. Classic Compute

**Deployment Model:**
- Clusters are provisioned directly within the **customer's cloud subscription**
- Compute resources (virtual machines) are deployed and managed within **your cloud account**

**Characteristics:**
- Traditional cluster provisioning approach
- Full control over compute resources
- Resources exist in your subscription

**Resource Location:** Customer Subscription

---

### 2. Serverless Compute

**Deployment Model:**
- Introduced in 2020
- Runs on the **Databricks subscription** rather than customer subscription
- Databricks allocates resources from its pool of pre-allocated virtual machines

**Advantages:**
- ⚡ **Significantly reduced cluster startup time**
- Leverages pre-allocated VMs within the Databricks subscription
- No need to provision new VMs from scratch
- Faster time to start processing

**Resource Location:** Databricks Subscription

---

## Workspace Cloud Storage

When you create a Databricks workspace, a default workspace cloud storage is automatically set up in your cloud subscription.

### Storage by Cloud Provider

| Cloud Provider | Storage Type |
|----------------|--------------|
| **Azure** | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| **AWS** | Amazon S3 Bucket |
| **GCP** | Google Cloud Storage (GCS) |

### What's Stored in Workspace Storage?

**System Data:**
- Notebook revisions
- Job run details
- Spark logs
- Workspace metadata

**Temporary Data:**
- User-generated temporary datasets
- Intermediate processing results

⚠️ **Important Consideration:**
- This storage is **tied to the workspace lifecycle**
- **All data will be deleted** when the workspace is deleted
- Not suitable for long-term production data storage

---

## Architecture Summary: Resource Distribution

Let's visualize where resources are located across subscriptions:

### Databricks Subscription

```
┌─────────────────────────────────────────┐
│     DATABRICKS SUBSCRIPTION             │
├─────────────────────────────────────────┤
│                                         │
│  ┌─────────────────────────────────┐   │
│  │     Control Plane               │   │
│  │  • Web UI                       │   │
│  │  • Cluster Manager              │   │
│  │  • Unity Catalog                │   │
│  │  • Query/Workspace Storage      │   │
│  └─────────────────────────────────┘   │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │   Serverless Compute            │   │
│  │  • Pre-allocated VMs            │   │
│  │  • Fast startup time            │   │
│  └─────────────────────────────────┘   │
│                                         │
└─────────────────────────────────────────┘
```

**Components in Databricks Subscription:**
- **Control Plane**: Handles essential tasks
  - User interface management
  - Compute orchestration through Cluster Manager
- **Serverless Compute**: Provisioned using pre-allocated VMs for faster startup

---

### Customer Subscription

```
┌─────────────────────────────────────────┐
│      YOUR CLOUD SUBSCRIPTION            │
├─────────────────────────────────────────┤
│                                         │
│  ┌─────────────────────────────────┐   │
│  │   Classic Compute Plane         │   │
│  │  • Customer-managed VMs         │   │
│  │  • Direct provisioning          │   │
│  └─────────────────────────────────┘   │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │   Workspace Cloud Storage       │   │
│  │  • System data                  │   │
│  │  • Temporary data               │   │
│  │  • Notebook revisions           │   │
│  │  • Job run details              │   │
│  └─────────────────────────────────┘   │
│                                         │
└─────────────────────────────────────────┘
```

**Components in Customer Subscription:**
- **Classic Compute Plane**: Provisioned directly in your cloud account when requested
- **Workspace Storage**: Holds system and temporary data

---

## Managed Resource Group (Azure Implementation)

When you create a Databricks service in Azure, Databricks automatically creates an additional resource group called the **Managed Resource Group**.

### Understanding the Managed Resource Group

⚠️ **Important Clarification:**
- The Managed Resource Group is **NOT part of the Databricks subscription**
- All resources in this group still live in **your subscription** (customer subscription)
- The difference is that **Databricks manages** these resources, not you
- This is separate from the Control Plane

### Resource Group Structure in Azure

```
YOUR AZURE SUBSCRIPTION
│
├─ Your Resource Group
│  └─ Databricks Workspace Resource
│
└─ Managed Resource Group (Created by Databricks)
   ├─ Storage Account (Workspace Storage)
   ├─ Virtual Network (VNet)
   ├─ Network Security Group (NSG)
   ├─ Azure Managed Identity
   ├─ Unity Catalog Access Connector (if enabled)
   └─ Virtual Machines (for Classic Compute)
```

### Resources in the Managed Resource Group

#### 1. Storage Account
- The workspace storage account we discussed earlier
- Stores system data, logs, notebook revisions, and temporary data
- **Location of workspace cloud storage**

#### 2. Virtual Network (VNet)
- Provides network isolation for your Databricks resources
- All resources are deployed within this secure network boundary

#### 3. Network Security Group (NSG)
- Controls inbound and outbound network traffic
- Provides security rules for your Databricks resources

#### 4. Azure Managed Identity
- Enables secure authentication to Azure resources
- Used for service-to-service authentication

#### 5. Unity Catalog Access Connector
- **Only present if Unity Catalog is enabled**
- Facilitates secure data access through Unity Catalog
- Workspaces created in the last year typically have this enabled automatically

**Note:** If you don't see the Unity Catalog Access Connector in your managed resource group, it simply means your workspace is not Unity Catalog-enabled. This is normal for older workspaces.

#### 6. Virtual Machines (for Classic Compute)
- **This is where all VMs for classic compute clusters are created**
- When you create a classic compute cluster, VMs are provisioned here
- All exist within your customer subscription

---

## Compute Provisioning: Where Resources Are Created

### Classic Compute Cluster Creation

When you create a **classic compute cluster**:

```
Your Azure Subscription
│
└─ Managed Resource Group
   └─ Virtual Machines (created here)
      ├─ Driver Node VM
      ├─ Worker Node VM 1
      ├─ Worker Node VM 2
      └─ Worker Node VM N
```

**Key Points:**
- VMs are created in the **Managed Resource Group**
- All resources exist in **your subscription**
- You are billed for these VMs in your subscription
- Databricks manages the lifecycle of these VMs

---

### Serverless Compute Cluster Creation

When you create a **serverless compute cluster**:

```
Databricks Subscription
│
└─ Databricks Managed Resources
   └─ Pre-allocated Virtual Machines
      ├─ Driver Node (from pool)
      ├─ Worker Node 1 (from pool)
      ├─ Worker Node 2 (from pool)
      └─ Worker Node N (from pool)
```

**Key Points:**
- VMs are allocated from Databricks' pre-allocated pool
- Resources exist in the **Databricks subscription**
- Faster startup time (no VM provisioning needed)
- Databricks manages all aspects of these resources

---

## Complete Architecture Diagram

Here's the complete picture showing all resource locations:

```
┌─────────────────────────────────────────────────────────────┐
│                 DATABRICKS SUBSCRIPTION                     │
├─────────────────────────────────────────────────────────────┤
│  Control Plane                                              │
│  • Web UI                                                   │
│  • Cluster Manager                                          │
│  • Unity Catalog                                            │
│                                                             │
│  Serverless Compute                                         │
│  • Pre-allocated VMs                                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              YOUR AZURE SUBSCRIPTION                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Your Resource Group                                        │
│  └─ Databricks Workspace Resource                          │
│                                                             │
│  Managed Resource Group (Databricks-managed)                │
│  ├─ Storage Account (Workspace Storage)                    │
│  ├─ Virtual Network                                         │
│  ├─ Network Security Group                                  │
│  ├─ Azure Managed Identity                                  │
│  ├─ Unity Catalog Access Connector (if enabled)            │
│  └─ Virtual Machines (Classic Compute Clusters)            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Access Capabilities

Through either compute model (Classic or Serverless), you can access and process data from:

✅ **Cloud Storage:**
- Azure Data Lake Storage
- Amazon S3
- Google Cloud Storage
- Other cloud storage services

✅ **On-Premises Applications:**
- Connect to on-premises databases
- Access enterprise data sources
- Hybrid cloud scenarios

---

## Compute Model Comparison

| Aspect | Classic Compute | Serverless Compute |
|--------|----------------|-------------------|
| **Location** | Customer Subscription (Managed Resource Group) | Databricks Subscription |
| **Provisioning** | Fresh VM allocation | Pre-allocated VMs |
| **Startup Time** | Slower (provision new VMs) | Faster (use existing VMs) |
| **VM Storage** | Managed Resource Group | Databricks-managed pool |
| **Billing** | Your subscription | Databricks subscription |
| **Introduced** | Original model | 2020 |
| **Management** | Databricks-managed (in your subscription) | Databricks-managed (in Databricks subscription) |
| **Best For** | Long-running workloads, predictable costs | Quick, on-demand processing |

---

## Key Takeaways

1. **Control Plane** (Databricks subscription) manages all platform services and orchestration
2. **Managed Resource Group** (your subscription) contains Databricks-managed resources:
   - Workspace storage account
   - Network components (VNet, NSG)
   - Classic compute VMs
   - Unity Catalog components (if enabled)
3. **Compute Plane** provides two options:
   - **Classic**: VMs created in Managed Resource Group (your subscription)
   - **Serverless**: VMs from Databricks subscription pool
4. **Workspace Storage** lives in the Managed Resource Group but is tied to workspace lifecycle
5. Both compute models can access cloud and on-premises data sources
6. Serverless compute offers significant performance benefits through pre-allocated resources

---

## Important Distinctions

**Control Plane vs. Managed Resource Group:**
- **Control Plane**: Lives in Databricks subscription, manages platform services
- **Managed Resource Group**: Lives in your subscription, contains Databricks-managed infrastructure resources

**Your Subscription Contains:**
- Databricks Workspace resource (in your resource group)
- Managed Resource Group with supporting infrastructure
- Classic compute VMs when created
- Workspace storage account

**Databricks Subscription Contains:**
- Control Plane services
- Serverless compute VMs
- Platform management infrastructure

---

Understanding this architecture helps you make informed decisions about resource allocation, cost management, security configuration, and performance optimization in your Databricks environment.