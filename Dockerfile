## THIS EXPECTS TO BE INVOKED AFTER A TAR BUILD.
## USE 'java -cp tools/tablesaw-1.2.4.jar make docker-image'
## TO BUILD.

FROM openjdk:8u151

ARG VERSION

ADD build/kairosdb-${VERSION}.tar /opt/

WORKDIR /opt/kairosdb

CMD ["/opt/kairosdb/bin/kairosdb.sh", "run"]

# Kairos API telnet and jetty ports
EXPOSE 4242 8083

LABEL maintainer="brianhks1+kairos@gmail.com" \
      org.label-schema.schema-version="1.0" \
      org.label-schema.name="kairosdb" \
      org.label-schema.description="KairosDB is a time series database that stores numeric values along\
 with key/value tags to a nosql data store.  Currently supported\
 backends are Cassandra and H2.  An H2 implementation is provided\
 for development work." \
      org.label-schema.docker.dockerfile="/Dockerfile"
