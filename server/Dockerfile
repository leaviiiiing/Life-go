# 可通过 compose build args 覆盖，便于国内网络走镜像加速（默认仍为 Docker Hub 官方名）
ARG GO_BUILD_IMAGE=golang:1.22-alpine
ARG RUNTIME_IMAGE=alpine:3.19
FROM ${GO_BUILD_IMAGE} AS build
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/hmdp-api ./cmd/server

FROM ${RUNTIME_IMAGE}
WORKDIR /app
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/hmdp-api /app/hmdp-api
ENV TZ=Asia/Shanghai
EXPOSE 8081 8082
ENTRYPOINT ["/app/hmdp-api"]
