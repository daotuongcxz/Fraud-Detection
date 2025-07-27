FROM python:3.8-slim

RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates && \
    curl -L https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" > /etc/apt/sources.list.d/corretto.list && \
    apt-get update && \
    apt-get install -y java-11-amazon-corretto-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java env
ENV JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "datastream_api.py"]
