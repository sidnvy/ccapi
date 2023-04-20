FROM mcr.microsoft.com/devcontainers/universal:2-linux
# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends swig cmake zlib1g-dev libssl-dev build-essential && \
    rm -rf /var/lib/apt/lists/*  

# Set the working directory
WORKDIR /app

# Copy the entire 'ccapi' directory into the container
COPY . .

RUN rm -fr binding/build

# Prepare the user_specified_cmake_include.cmake file
COPY binding/user_specified_cmake_include.cmake.example /app/user_specified_cmake_include.cmake

# Uncomment all lines except those containing "FIX" and "EXECUTION_MANAGEMENT"
RUN sed -i'' -E '/(FIX|EXECUTION|TRACE|DEBUG|INFO)/! s/^# //' /app/user_specified_cmake_include.cmake

# Build the ccapi library with Python bindings
RUN mkdir -p binding/build && \
    cd binding/build && \
    cmake -DCMAKE_PROJECT_INCLUDE=/app/user_specified_cmake_include.cmake -DBUILD_VERSION=latest -DBUILD_PYTHON=ON -DINSTALL_PYTHON=ON .. && \
    cmake --build . && \
    cmake --install . &&\
    pip install --no-cache-dir python-dateutil pyarrow boto3 clickhouse-driver