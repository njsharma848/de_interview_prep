We already know that the Spark application runs multiple jobs. Within a single Spark application, each of your actions triggers a spark job. In a typical case, these spark jobs run sequentially. What does it mean? I mean, the spark will start Job 1 and finish it. Then only Job 2 starts. That's what I mean by running Spark jobs in a sequence.

However, you can also trigger them to run parallelly.

# Spark Job Execution Architecture

## Workflow Structure
```
Application (Sequential)
│
├─ Job 1
│  ├─ Stage 1 → Parallel Tasks
│  └─ Stage 2 → Parallel Tasks
│
└─ Job 2 (Sequential)
   ├─ Stage 1 → Parallel Tasks
   ├─ Stage 2 → Parallel Tasks
   └─ Stage 3 → Parallel Tasks
```

## Key Components

- **Application**: The entry point that submits jobs

- **Job 1**: Contains 2 stages
  - Stage 1 → Parallel Tasks
  - Stage 2 → Parallel Tasks

- **Job 2**: Contains 3 stages
  - Stage 1 → Parallel Tasks
  - Stage 2 → Parallel Tasks
  - Stage 3 → Parallel Tasks

## Execution Model

1. Jobs are executed **sequentially** (Job 1 completes before Job 2 starts)
2. Within each job, stages are executed based on dependencies
3. Each stage spawns **parallel tasks** that run across the cluster