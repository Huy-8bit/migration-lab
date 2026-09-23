FROM confluentinc/cp-kafka-connect:7.6.0

USER root
RUN mkdir -p /usr/share/java/debezium-connector-postgres && \
    curl -sfSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.4.Final/debezium-connector-postgres-2.5.4.Final-plugin.tar.gz | tar -xzf - -C /usr/share/java/debezium-connector-postgres --strip-components=1 && \
    chmod -R 755 /usr/share/java/debezium-connector-postgres
USER appuser
