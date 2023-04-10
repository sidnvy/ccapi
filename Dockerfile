# First stage: build the ccapi library with Python bindings
FROM python:3.9-slim-bullseye AS builder

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends swig cmake zlib1g-dev libssl-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the entire 'ccapi' directory into the container
COPY . .

# Prepare the user_specified_cmake_include.cmake file
COPY binding/user_specified_cmake_include.cmake.example /app/user_specified_cmake_include.cmake

# Uncomment all lines except those containing "FIX" and "EXECUTION_MANAGEMENT"
RUN sed -i'' -E '/(FIX|EXECUTION|TRACE|DEBUG|INFO)/! s/^# //' binding/user_specified_cmake_include.cmake

# Build the ccapi library with Python bindings
RUN mkdir -p binding/build && \
    cd binding/build && \
    cmake -DCMAKE_PROJECT_INCLUDE=/app/user_specified_cmake_include.cmake -DBUILD_VERSION=latest -DBUILD_PYTHON=ON -DINSTALL_PYTHON=ON .. && \
    cmake --build . && \
    cmake --install .

# Second stage: create the final image with necessary runtime dependencies
FROM python:3.9-slim-bullseye

# Install necessary runtime dependencies and additional Python packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl1.1 && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir python-dateutil pyarrow boto3

# Copy Python bindings and libraries from the builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/lib /usr/local/lib

# Set environment variable for Python bindings
ENV PYTHONPATH="/usr/local/lib/python3.9/site-packages:${PYTHONPATH}"

# Copy the collector.py script to the container
COPY collector.py /app/collector.py

# Set the default command for the container to run the collector.py script
CMD ["python", "/app/collector.py"]

