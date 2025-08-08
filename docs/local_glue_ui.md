# Local Glue UI Access

This document explains how to connect to the AWS Glue UI when running jobs locally using Docker.

## Available UIs

The local Docker container exposes two web interfaces for monitoring and debugging:

### Spark UI (Port 4040)
- **URL**: http://localhost:4040
- **Purpose**: Monitor Spark job execution, stages, tasks, and performance metrics
- **When Available**: Only when a Spark job is actively running
- **Use Cases**:
  - Debug slow-running transformations
  - Monitor data processing stages
  - View SQL execution plans
  - Analyze resource usage

### Glue Dev Endpoint UI (Port 8080)
- **URL**: http://localhost:8080  
- **Purpose**: Glue development environment interface
- **When Available**: When the container is running
- **Use Cases**:
  - Interactive notebook development
  - Job debugging and development

## Setup and Access

### 1. Start the Local Environment
```bash
# Start container in detached mode (stays running)
cd glue-jobs
docker-compose up -d

# Or use make command (for running specific jobs)
make run-local JOB=<job_name>
```

### 2. Access the UIs
Once the container is running:

- **Spark UI**: Open http://localhost:4040 in your browser
- **Glue Dev UI**: Open http://localhost:8080 in your browser

### 3. Monitor Job Execution
```bash
# Run a job and monitor via Spark UI
make run-local JOB=customer_import

# In another terminal, access the UIs while job is running
open http://localhost:4040  # Spark UI (macOS)
open http://localhost:8080  # Glue Dev UI (macOS)
```

## Troubleshooting

### Port Already in Use
If ports are already in use, check for existing containers:
```bash
docker ps -a
docker-compose down  # Stop any existing containers
```

### UI Not Accessible
1. Verify container is running: `docker ps`
2. Check container logs: `docker logs glue-local`
3. Ensure ports are properly mapped in docker-compose.yml

### Spark UI Shows "No Running Applications"
The Spark UI only appears when jobs are actively running. Start a job execution to see the interface.

## Configuration

The UI ports are configured in `glue-jobs/docker-compose.yml`:
```yaml
ports:
  - "4040:4040"  # Spark UI
  - "8080:8080"  # Glue Dev Endpoint UI
```

To change ports, modify the docker-compose.yml file and restart the container.