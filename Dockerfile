FROM golang:1.27.3-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOPROXY=https://goproxy.cn,direct
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server && go build -trimpath -ldflags="-s -w" -o /out/import ./cmd/import
FROM alpine:3.23
RUN apk add --no-cache ca-certificates tzdata && addgroup -g 10001 core && adduser -D -u 10001 -G core core && mkdir -p /data/images && chown -R core:core /data
COPY --from=build /out/ /usr/local/bin/
USER core
ENV CORE_LISTEN=:9080 CORE_IMAGE_DIR=/data/images TZ=Asia/Shanghai
EXPOSE 9080
ENTRYPOINT ["/usr/local/bin/server"]
