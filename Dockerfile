FROM alpine:3.13 AS build
RUN apk add --update build-base
WORKDIR /csvdb
COPY . .
RUN make cgi

FROM sebp/lighttpd
COPY ./lighttpd/lighttpd.conf /etc/lighttpd/
COPY ./lighttpd/htdocs /var/www/html
COPY --from=build /csvdb/release/csvdb.cgi /var/www/html/cgi-bin/csvdb
COPY --from=build /csvdb/suits.csv /var/www/html/cgi-bin/
COPY --from=build /csvdb/ranks.csv /var/www/html/cgi-bin/