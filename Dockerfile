FROM debian

# Copy everything from the current directory into the container
COPY . .

# Update and install Java
RUN apt-get update && \
    apt-get install -y default-jre || exit 1

# Dynamically determine and set JAVA_HOME
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f /usr/bin/java))) && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment

# Verify Java Installation
RUN java -version || exit 1

# Install Python and create a virtual environment
RUN apt-get install -y python3 python3-venv || exit 1
RUN python3 -m venv /opt/venv

# Activate the virtual environment and install PySpark
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install pyspark || exit 1

# Command to run the script
CMD ["python", "Part5.py"]
