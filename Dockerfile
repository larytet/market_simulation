FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/simulate ./cmd/simulate && CGO_ENABLED=0 go build -o /out/algo ./cmd/algo

FROM gcr.io/distroless/static-debian12 AS simulate
COPY --from=build /out/simulate /simulate
ENTRYPOINT ["/simulate"]

FROM gcr.io/distroless/static-debian12 AS algo
COPY --from=build /out/algo /algo
ENTRYPOINT ["/algo"]
