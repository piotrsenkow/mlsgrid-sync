FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-X github.com/piotrsenkow/mlsgrid-sync/internal/cli.version=${VERSION}" \
    -o /out/mlsgrid-sync ./cmd/mlsgrid-sync

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 mlsgrid
USER mlsgrid
COPY --from=builder /out/mlsgrid-sync /usr/local/bin/mlsgrid-sync
ENTRYPOINT ["mlsgrid-sync"]
