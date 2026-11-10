FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /queue ./cmd/queue

FROM scratch
COPY --from=build /queue /queue
USER 65532:65532
ENTRYPOINT ["/queue"]
