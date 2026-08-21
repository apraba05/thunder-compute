# Tiny runtime image — binary only, no build toolchain inside the cluster.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY vgpu-scheduler /usr/local/bin/vgpu-scheduler
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/vgpu-scheduler"]
