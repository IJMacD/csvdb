FROM alpine:3.13 AS build
RUN apk add --update build-base
WORKDIR /csvdb
COPY makefile ./
COPY src/ ./src/
RUN make cgi

FROM sebp/lighttpd
COPY ./lighttpd/lighttpd.conf /etc/lighttpd/
COPY ./lighttpd/htdocs /var/www/html
COPY --from=build /csvdb/release/csvdb.cgi /var/www/html/cgi-bin/csvdb