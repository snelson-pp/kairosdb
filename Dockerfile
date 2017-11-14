## THIS EXPECTS TO BE INVOKED AFTER A TAR BUILD.
## USE 'java -cp tools/tablesaw-1.2.4.jar make docker-image'
## TO BUILD.

FROM java:openjdk-8

ARG VERSION

ADD build/kairosdb-${VERSION}.tar /root/

WORKDIR /root/kairosdb

CMD ["/root/kairosdb/bin/kairosdb.sh", "run"]

# Kairos API telnet and jetty ports
EXPOSE 4242 8083

LABEL maintainer="brianhks1+kairos@gmail.com" \
      org.label-schema.schema-version="1.0" \
      org.label-schema.name="kairosdb" \
      org.label-schema.description="Test version for kairosdb" \
      org.label-schema.docker.dockerfile="/Dockerfile"
