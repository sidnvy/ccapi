FROM mcr.microsoft.com/devcontainers/universal:2-linux
# Install the required packages for building Python with shared library support
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        zlib1g-dev \
        libncurses5-dev \
        libncursesw5-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        libbz2-dev \
        liblzma-dev \
        tk-dev \
        uuid-dev

# Download and extract Python 3.10 source code
RUN cd /tmp && \
    curl -O https://www.python.org/ftp/python/3.10.0/Python-3.10.0.tgz && \
    tar -xzf Python-3.10.0.tgz && \
    cd Python-3.10.0

# Configure, build and install Python with shared library support
RUN cd /tmp/Python-3.10.0 && \
    ./configure --enable-shared && \
    make && \
    make install

# Clean up the temporary files
RUN rm -rf /tmp/*

# Set the default Python version to 3.10
RUN update-alternatives --install /usr/bin/python python /usr/local/bin/python3.10 1

# Build the ccapi library with Python bindings
RUN pip install --no-cache-dir python-dateutil pyarrow boto3 clickhouse-driver