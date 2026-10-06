FROM alpine:3.24 AS build
RUN apk add --no-cache build-base
WORKDIR /csvdb
COPY makefile ./
COPY src/ ./src/
ARG CSVDB_VERSION="debug"
RUN make CSVDB_VERSION=${CSVDB_VERSION}
RUN make cgi CSVDB_VERSION=${CSVDB_VERSION}

FROM sebp/lighttpd
ARG CSVDB_VERSION="debug"
LABEL org.opencontainers.image.title="csvdb" \
      org.opencontainers.image.description="SQL for CSV files" \
      org.opencontainers.image.version="${CSVDB_VERSION}"
ENV CSVDB_DATA_DIR="/data"
WORKDIR /data
EXPOSE 80
COPY ./lighttpd/startup.sh /startup.sh
COPY ./lighttpd/docker-entrypoint.sh /docker-entrypoint.sh
COPY ./lighttpd/lighttpd.conf /etc/lighttpd/
COPY ./lighttpd/htdocs /var/www/html
COPY --from=build /csvdb/cgi/csvdb.cgi /var/www/html/cgi-bin/csvdb
COPY --from=build /csvdb/release/csvdb /usr/bin/csvdb
RUN chmod +x /startup.sh /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/startup.sh"]
