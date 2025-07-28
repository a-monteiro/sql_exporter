FROM public.ecr.aws/docker/library/golang:1.23 AS builder

ENV USER=app
ENV UID=10001
# See https://stackoverflow.com/a/55757473/12429735RUN
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

ADD .   /go/src/github.com/a-monteiro/sql_exporter
WORKDIR /go/src/github.com/a-monteiro/sql_exporter

#RUN make drivers-custom
RUN make build
RUN chmod +x ./sql_exporter

# Make image and copy build sql_exporter
FROM scratch

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/src/github.com/a-monteiro/sql_exporter/sql_exporter  /bin/sql_exporter

EXPOSE 9100
USER app:app
ENTRYPOINT [ "/bin/sql_exporter" ]
