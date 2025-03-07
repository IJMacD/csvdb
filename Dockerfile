FROM alpine:3.13 AS build
RUN apk add --update build-base
WORKDIR /csvdb
COPY makefile ./
COPY src/ ./src/
ARG CSVDB_VERSION
RUN make cgi CSVDB_VERSION=${CSVDB_VERSION}

FROM sebp/lighttpd
ENV CSVDB_DATA_DIR="/data"
COPY ./lighttpd/startup.sh /startup.sh
COPY ./lighttpd/lighttpd.conf /etc/lighttpd/
COPY ./lighttpd/htdocs /var/www/html
COPY --from=build /csvdb/cgi/csvdb.cgi /var/www/html/cgi-bin/csvdb
CMD ["/startup.sh"]