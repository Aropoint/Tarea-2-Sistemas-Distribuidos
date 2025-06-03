FROM openjdk:8-jdk

ENV HADOOP_VERSION=3.3.6
ENV PIG_VERSION=0.17.0
ENV HADOOP_HOME=/opt/hadoop
ENV PIG_HOME=/opt/pig
ENV JAVA_HOME=/usr/local/openjdk-8
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin:$JAVA_HOME/bin

# Instalar dependencias
RUN apt-get update && apt-get install -y \
    ssh \
    openssh-server \
    wget \
    vim \
    net-tools \
    sudo \
    procps \
    && apt-get clean

# Configurar SSH
RUN mkdir /var/run/sshd && \
    ssh-keygen -A && \
    sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

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

# Descargar piggybank.jar (versión corregida)
RUN mkdir -p ${PIG_HOME}/lib && \
    wget -P ${PIG_HOME}/lib/ https://repo1.maven.org/maven2/org/apache/pig/piggybank/${PIG_VERSION}/piggybank-${PIG_VERSION}.jar && \
    (rm -f ${PIG_HOME}/lib/piggybank.jar || true) && \
    ln -s ${PIG_HOME}/lib/piggybank-${PIG_VERSION}.jar ${PIG_HOME}/lib/piggybank.jar

# Descargar json-simple.jar (nuevo)
RUN wget -P ${PIG_HOME}/lib/ https://repo1.maven.org/maven2/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar && \
    ln -s ${PIG_HOME}/lib/json-simple-1.1.1.jar ${PIG_HOME}/lib/json-simple.jar

# Configurar variables de entorno para Pig (modificado para incluir json-simple)
ENV PIG_CLASSPATH=${HADOOP_HOME}/etc/hadoop:${PIG_HOME}/lib/piggybank.jar:${PIG_HOME}/lib/json-simple.jar
ENV PIG_OPTS="-Dpig.additional.jars=${PIG_HOME}/lib/piggybank.jar,${PIG_HOME}/lib/json-simple.jar"

# Verificar que los archivos existen
RUN ls -lh ${PIG_HOME}/lib/ && \
    test -f ${PIG_HOME}/lib/piggybank.jar && \
    test -f ${PIG_HOME}/lib/json-simple.jar

# Configurar Hadoop
COPY core-site.xml $HADOOP_HOME/etc/hadoop/
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/
COPY mapred-site.xml $HADOOP_HOME/etc/hadoop/
COPY yarn-site.xml $HADOOP_HOME/etc/hadoop/

RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_HOME=$HADOOP_HOME" >> ~/.bashrc && \
    echo "export PATH=$PATH" >> ~/.bashrc

# Crear usuario hadoop
RUN useradd -m -s /bin/bash hadoop && \
    echo "hadoop:hadoop" | chpasswd && \
    adduser hadoop sudo && \
    echo 'hadoop ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    chown -R hadoop:hadoop $HADOOP_HOME

USER hadoop
WORKDIR /home/hadoop

# Configurar SSH sin contraseña
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 600 ~/.ssh/authorized_keys

# Copiar scripts
COPY --chown=hadoop:hadoop entrypoint.sh /entrypoint.sh
COPY --chown=hadoop:hadoop check_and_upload_csv.sh /home/hadoop/
COPY --chown=hadoop:hadoop scripts/ /scripts/

RUN chmod +x /entrypoint.sh /home/hadoop/check_and_upload_csv.sh

ENTRYPOINT ["/entrypoint.sh"]