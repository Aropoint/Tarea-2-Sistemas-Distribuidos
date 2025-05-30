FROM openjdk:8-jdk

# Variables de entorno
ENV HADOOP_VERSION=3.3.6
ENV PIG_VERSION=0.17.0
ENV HADOOP_HOME=/opt/hadoop
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin

RUN apt-get update && apt-get install -y \
    ssh \
    wget \
    vim \
    net-tools \
    && apt-get clean

# Instalar Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Instalar Pig
RUN wget https://downloads.apache.org/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz -C /opt/ && \
    mv /opt/pig-${PIG_VERSION} ${PIG_HOME} && \
    rm pig-${PIG_VERSION}.tar.gz

# Configuraciones básicas para que funcione pseudo-distribuido
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Crear usuario para Hadoop (opcional)
RUN useradd -ms /bin/bash hadoop && echo "hadoop:hadoop" | chpasswd && adduser hadoop sudo
USER hadoop
WORKDIR /home/hadoop

ENTRYPOINT ["/entrypoint.sh"]
