# Imagen Hadoop reutilizada del laboratorio de la asignatura (LABORATORIO_ALUMNOS_HDFS/hadoop).
FROM eclipse-temurin:11-jre-jammy
ARG HADOOP_VERSION=3.4.2
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates python3 && rm -rf /var/lib/apt/lists/*
RUN curl -fSL --retry 3 https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -o /tmp/hadoop.tgz \
 && curl -fSL --retry 3 https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512 -o /tmp/hadoop.sha512 \
 && cd /tmp && expected="$(grep -Eo '[a-fA-F0-9]{128}' hadoop.sha512 | head -1)" \
 && test -n "$expected" && echo "$expected  hadoop.tgz" | sha512sum -c - \
 && tar -xzf hadoop.tgz -C /opt && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm /tmp/hadoop.tgz /tmp/hadoop.sha512
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV HADOOP_HEAPSIZE_MAX=512
COPY core-site.xml hdfs-site.xml mapred-site.xml /opt/hadoop/etc/hadoop/
COPY start.sh /start.sh
RUN chmod +x /start.sh
ENTRYPOINT ["/start.sh"]
