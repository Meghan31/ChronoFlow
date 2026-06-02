# ChronoFlow

A distributed task scheduling and workflow orchestration system built with C++ backend services, gRPC communication, and a modern React TypeScript frontend.

## Project Overview

ChronoFlow is a microservice-based task scheduling platform that enables:

- **Distributed Task Scheduling**: Submit and manage tasks across multiple worker nodes
- **Dependency Management**: Support for task dependencies and DAG execution
- **Health Monitoring**: Track worker health and task execution status
- **Retry Logic**: Automatic retry mechanisms for failed tasks
- **Real-time Dashboard**: Monitor tasks, workers, and system metrics

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Frontend (React/TS)                 │
│                                                          │
│              Dashboard & Monitoring UI                  │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP/WebSocket
┌────────────────────▼────────────────────────────────────┐
│                  API Gateway (Node.js)                  │
│                                                          │
│          REST & WebSocket Proxy → gRPC                  │
└────────────────────┬────────────────────────────────────┘
                     │ gRPC
        ┌────────────┴────────────┐
        │                         │
┌───────▼──────────┐    ┌─────────▼──────────┐
│ Scheduler Service│    │ Worker Service(s)  │
│     (C++)        │    │     (C++)          │
│                  │    │                    │
│ • DAG Engine     │    │ • Task Execution   │
│ • Task Queue     │    │ • Health Monitor   │
│ • Retry Manager  │    │ • Status Report    │
│ • Worker Registry│    │                    │
└───────┬──────────┘    └─────────┬──────────┘
        │                         │
        └────────────┬────────────┘
                     │ SQLite
            ┌────────▼────────┐
            │ Metadata Store  │
            │                 │
            │ • Tasks         │
            │ • Workers       │
            │ • Execution Log │
            └─────────────────┘
```

## Prerequisites

### Required

- **CMake** >= 3.20
- **C++20** compatible compiler (clang, gcc, or MSVC)
- **gRPC** and **Protocol Buffers**
- **SQLite3**
- **spdlog** (logging library)
- **Google Test** (for testing)
- **Node.js** >= 16 (for frontend and gateway)
- **Docker** & **Docker Compose** (optional, for containerized deployment)

### macOS Setup with Homebrew

```bash
# Install dependencies
brew install cmake protobuf grpc sqlite3 spdlog

# Install Node.js (if not already installed)
brew install node
```

### Linux Setup (Ubuntu/Debian)

```bash
# Update package manager
sudo apt update

# Install build tools
sudo apt install -y build-essential cmake git

# Install dependencies
sudo apt install -y \
    libgrpc-dev \
    libprotobuf-dev \
    protobuf-compiler-grpc \
    sqlite3 \
    libsqlite3-dev \
    libspdlog-dev \
    googletest \
    libgtest-dev

# Install Node.js
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs
```

## Build Instructions

### 1. Generate Protocol Buffers

```bash
cd ChronoFLow
mkdir -p build
cd build

# Generate gRPC and Protobuf files
protoc --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` ../proto/*.proto
protoc --cpp_out=. ../proto/*.proto
```

### 2. Build C++ Services with CMake

```bash
cd ChronoFLow/build

# Configure build
cmake ..

# Build all targets
cmake --build .

# Or use make directly
make -j$(nproc)
```

This generates:

- `scheduler_service` - Main scheduler server
- `worker_service` - Worker node service
- `run_tests` - Unit test executable

### 3. Build Frontend

```bash
cd frontend-cf

# Install dependencies
npm install

# Build for production
npm run build

# Or run development server
npm run dev
```

### 4. Build API Gateway

```bash
cd gateway

# Install dependencies
npm install

# Build (if needed)
npm run build

# Or run in development mode
npm run dev
```

## Running the Project

### Option 1: Run with Docker Compose (Recommended)

```bash
# From the root directory
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

This will start:

- Scheduler Service on `localhost:50051`
- Worker Service on `localhost:50052`
- API Gateway on `localhost:3000`
- Frontend UI on `localhost:8080`

### Option 2: Run Services Manually

#### Terminal 1 - Start Scheduler Service

```bash
cd ChronoFLow/build
./scheduler_service
```

Expected output:

```
[scheduler] Starting Scheduler Service on 0.0.0.0:50051
[scheduler] Health Monitor initialized
```

#### Terminal 2 - Start Worker Service

```bash
cd ChronoFLow/build
./worker_service
```

Expected output:

```
[worker] Starting Worker Service
[worker] Connecting to Scheduler at localhost:50051
```

#### Terminal 3 - Start API Gateway

```bash
cd gateway
npm run dev
```

Gateway will be available at `http://localhost:3000`

#### Terminal 4 - Start Frontend

```bash
cd frontend-cf
npm run dev
```

Frontend will be available at `http://localhost:5173` (Vite default)

### Running Tests

```bash
cd ChronoFLow/build
./run_tests

# Or with verbose output
./run_tests --verbose
```

## Configuration

### Environment Variables

Create a `.env` file in the root directory:

```bash
# Scheduler Configuration
SCHEDULER_HOST=0.0.0.0
SCHEDULER_PORT=50051
SCHEDULER_METRICS_PORT=50053

# Worker Configuration
WORKER_SCHEDULER_HOST=localhost
WORKER_SCHEDULER_PORT=50051
WORKER_THREADS=4

# Database
DATABASE_PATH=./metadata.db

# Logging
LOG_LEVEL=info
LOG_FILE=./logs/chronoflow.log

# Frontend
VITE_API_URL=http://localhost:3000
VITE_WS_URL=ws://localhost:3000
```

## Project Structure

```
ChronoFLow/
├── include/               # Header files
│   ├── concurrent_queue.h # Thread-safe queue
│   └── task.h            # Task definitions
├── proto/                # Protocol Buffer definitions
│   ├── scheduler.proto   # Scheduler service RPC definitions
│   └── worker.proto      # Worker service RPC definitions
├── src/
│   ├── common/           # Shared utilities
│   │   ├── task.cpp
│   │   └── thread_pool.cpp
│   ├── scheduler/        # Scheduler service implementation
│   │   ├── dag_engine.cpp       # Directed Acyclic Graph execution
│   │   ├── health_monitor.cpp   # Worker health tracking
│   │   ├── retry_manager.cpp    # Task retry logic
│   │   ├── scheduler_server.cpp # gRPC server setup
│   │   └── worker_registry.cpp  # Worker registration & discovery
│   ├── storage/          # Data persistence
│   │   └── metadata_store.cpp   # SQLite database layer
│   └── worker/           # Worker service implementation
│       └── worker.cpp    # Task execution engine
├── tests/                # Unit tests
│   └── test_dummy.cpp
├── build/                # CMake build output
├── docker/               # Container definitions
└── CMakeLists.txt        # Build configuration

frontend-cf/             # React TypeScript frontend
├── src/
│   ├── components/      # UI components
│   ├── pages/          # Page components
│   ├── api/            # API client functions
│   ├── store/          # State management
│   └── types/          # TypeScript types
└── package.json

gateway/                # API Gateway (Node.js)
├── src/
│   ├── routes/        # HTTP routes
│   ├── grpc/          # gRPC client setup
│   └── ws/            # WebSocket handlers
└── package.json
```

## API Endpoints

### Task Management

```bash
# Submit a new task
POST /api/tasks
{
  "name": "process_data",
  "type": "COMPUTE",
  "dependencies": ["task_id_1"],
  "timeout": 300
}

# Get task status
GET /api/tasks/:taskId

# List all tasks
GET /api/tasks?status=running&limit=50

# Cancel a task
DELETE /api/tasks/:taskId
```

### Worker Management

```bash
# Get registered workers
GET /api/workers

# Get worker details
GET /api/workers/:workerId

# Get worker metrics
GET /api/workers/:workerId/metrics
```

### System Metrics

```bash
# Get system overview
GET /api/metrics/overview

# Get task statistics
GET /api/metrics/tasks

# Get worker statistics
GET /api/metrics/workers
```

## WebSocket Events

Real-time updates via WebSocket connection:

```javascript
// Connect
const ws = new WebSocket('ws://localhost:3000/ws');

// Listen for task updates
ws.onmessage = (event) => {
	const message = JSON.parse(event.data);
	// message.type: 'task_status_changed', 'worker_joined', etc.
	// message.data: detailed event information
};
```

## Troubleshooting

### Build Issues

**CMake not finding gRPC**

```bash
# Set CMake prefix path
export CMAKE_PREFIX_PATH="/opt/homebrew:$CMAKE_PREFIX_PATH"
cmake ..
```

**Protobuf compilation errors**

```bash
# Verify protoc version
protoc --version
grpc_cpp_plugin --version

# Regenerate protobuf files
protoc --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` ../proto/*.proto
protoc --cpp_out=. ../proto/*.proto
```

### Runtime Issues

**"Connection refused" between services**

- Ensure Scheduler Service is running first
- Check `WORKER_SCHEDULER_HOST` and `WORKER_SCHEDULER_PORT` in environment
- Verify firewall allows communication on ports 50051-50053

**Frontend can't connect to gateway**

- Check gateway is running on port 3000
- Verify `VITE_API_URL` in frontend .env matches gateway URL
- Check browser console for CORS errors

**Database lock errors**

- Stop all services
- Delete stale database lock: `rm metadata.db-wal`
- Restart services

## Development Workflow

### Adding a New Task Type

1. Define in `proto/scheduler.proto`:

   ```protobuf
   enum TaskType {
     COMPUTE = 0;
     YOUR_NEW_TYPE = 1;
   }
   ```

2. Regenerate protobuf files
3. Implement handler in `src/scheduler/scheduler_service_impl.cpp`
4. Add tests in `tests/`
5. Rebuild and test

### Adding Frontend Features

1. Create component in `frontend-cf/src/components/`
2. Add types in `frontend-cf/src/types/`
3. Add API functions in `frontend-cf/src/api/`
4. Integrate into page/layout
5. Test with development server

## Performance Tuning

### Scheduler Optimization

- Adjust DAG engine thread pool: `src/scheduler/dag_engine.h`
- Tune retry backoff: `src/scheduler/retry_manager.h`
- Configure worker registry: `src/scheduler/worker_registry.h`

### Worker Optimization

- Increase worker threads: Set `WORKER_THREADS` environment variable
- Configure batch sizes in `src/worker/worker.cpp`

### Database Optimization

- Index frequently queried columns in `src/storage/metadata_store.cpp`
- Monitor database file size: `ls -lh metadata.db`

## Contributing

1. Create a feature branch
2. Make your changes
3. Run tests: `./build/run_tests`
4. Format code: Use clang-format for C++
5. Submit pull request

## License

[Add your license here]

## Support

For issues, questions, or contributions, please create an issue or contact the development team.
