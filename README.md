# kong-kafka-request-logger-plugin

Plugin "as is" tested with kong 2.6.0 >= x < 3.2.2.

# What's this?

This is a basic plugin for [Kong Gateway](https://github.com/Kong/kong) (OSS) using Go and [Kong-PDK](https://github.com/Kong/go-pdk).

It's job is to create request and response logs with a trace-id in a kafka topic for further processing by a logger microsservice.

It's done in Go for it's speed and because Go is fun.
