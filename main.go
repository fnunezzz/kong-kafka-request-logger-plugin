package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/Kong/go-pdk"
	"github.com/Kong/go-pdk/server"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Kong variables
const Version = "1.0.0"
const Priority = 100

// Default values if not informed (optional)
const DEFAULT_TOPIC_NAME = "kong-kafka-request-logger-plugin"
const DEFAULT_TRACE_HEADER_NAME = "trace-id"

type requestJsonMessage struct {
	TraceId  interface{} `json:"trace-id"`
	Request  interface{} `json:"request"`
	Date  interface{} `json:"date"`
}
type responseJsonMessage struct {
	TraceId  interface{} `json:"trace-id"`
	Response  interface{} `json:"response"`
	Date  interface{} `json:"date"`
}

type Config struct {
	HeaderName string `kong:"help:'Header name for trace-id',default:'trace-id',optional"`
	Topic string `kong:"help:'Topic name',default:'kong-kafka-request-logger-plugin',optional"`
	KafkaBrokers string `kong:"required,help:'List of Kafka brokers separated by comma',placeholder:'broker1:port,broker2:port'"`
}


func main() {
	server.StartServer(New, Version, Priority)
}


func New() interface{} {
	return &Config{}
}


//Access is the hook that is triggered on the `access` phase of the request
func (conf Config) Access(kong *pdk.PDK) {
	timestamp := time.Now()
	// Kafka Broker Validation
	// If no kafka brokers are informed - returns
	if (conf.KafkaBrokers == "") {
		kong.Log.Err("[REQUEST] No kafka brokers informed")
		return
	}

	// Default Header to set topic names
	if (conf.Topic == "") {
		kong.Log.Notice("[REQUEST] Topic name not set - Setting default " + DEFAULT_TOPIC_NAME)
		conf.Topic = DEFAULT_TOPIC_NAME
	}
	kong.Log.Notice("[REQUEST] Topic name: " + conf.Topic)

	// Default Header to set traceId
	if (conf.HeaderName == "") {
		kong.Log.Notice("[REQUEST] Trace header name not set - Setting default " + DEFAULT_TRACE_HEADER_NAME)
		conf.HeaderName = DEFAULT_TRACE_HEADER_NAME
	} else {
		conf.HeaderName = strings.Trim(conf.HeaderName, "")
	}

	kong.Log.Notice("[REQUEST] Trace id header name: " + conf.HeaderName)

	// Enable access to Service body and response headers at the same time
	enable, err := kong.Request.Ask("kong.service.request.enable_buffering")
	if (err != nil) {
		kong.Log.Err(err)
	} else {
		kong.Log.Notice("[REQUEST] Enabling body and headers access ", enable)
	}

	// Add a trace-id if doesn't exists
	traceId, err := kong.Request.GetHeader(conf.HeaderName)
	if (err != nil || strings.Trim(traceId, "") == "") {
		kong.Log.Warn(err)
		traceId = uuid.NewString()
		kong.ServiceRequest.SetHeader(conf.HeaderName, traceId)
	}
	// Gets the request path
	// It is the path that the client requested to Kong API Gateway
	path, err := kong.Request.GetPath()
	if err != nil {
		kong.Log.Warn(err)
	}

	// Gets the raw body
	body, err := kong.Request.GetRawBody()
	if err != nil {
		kong.Log.Warn(err)
	}

	// Gets http method GET, POST, PUT, DEL...
	httpMethod, _ := kong.Request.GetMethod()
	if err != nil {
		kong.Log.Warn(err)
	}

	// Gets protocl http or https
	httpProtocol, err := kong.Request.GetScheme()
	if err != nil {
		kong.Log.Warn(err)
	}

	// Gets all the headers
	headers, err := kong.Request.GetHeaders(99)
	if err != nil {
		kong.Log.Warn(err)
	}

	request := make(map[string]interface{})

	request["path"] = path
	request["httpProtocol"] = httpProtocol
	request["httpMethod"] = httpMethod
	request["body"] = body
	request["headers"] = headers

	raw := requestJsonMessage{
		Request: request,
		TraceId: traceId,
		Date: timestamp,
	}

	go sendMessage(kong, raw, conf.Topic, conf.KafkaBrokers)

}


//Response is the hook that is triggered on the `response` phase of the request
func (conf Config) Response(kong *pdk.PDK) {
	timestamp := time.Now()
	// Kafka Broker Validation
	// If no kafka brokers are informed - returns
	if (conf.KafkaBrokers == "") {
		kong.Log.Err("[RESPONSE] No kafka brokers informed")
		return
	}

	// Default Header to set topic names
	if (conf.Topic == "") {
		kong.Log.Notice("[RESPONSE] Topic name not set - Setting default " + DEFAULT_TOPIC_NAME)
		conf.Topic = DEFAULT_TOPIC_NAME
	}
	kong.Log.Notice("[RESPONSE] Topic name: " + conf.Topic)
	// Default Header to set traceId
	if (conf.HeaderName == "") {
		kong.Log.Notice("[RESPONSE] Trace header name not set - Setting default " + DEFAULT_TRACE_HEADER_NAME)
		conf.HeaderName = DEFAULT_TRACE_HEADER_NAME
	} else {
		conf.HeaderName = strings.Trim(conf.HeaderName, "")
	}

	kong.Log.Notice("[RESPONSE] Trace id header name: " + conf.HeaderName)

	// Add a trace-id if doesn't exists
	traceId, err := kong.Request.GetHeader(conf.HeaderName)
	if (err != nil  || strings.Trim(traceId, "") == "") {
		kong.Log.Warn(err)
		traceId = uuid.NewString()
	}
	
	kong.Response.SetHeader(conf.HeaderName, traceId)

	// Gets the request path
	// It is the path that the client requested to Kong API Gateway
	path, err := kong.Request.GetPath()
	if err != nil {
		kong.Log.Warn(err)
	}

	// Gets the raw body
	body, err := kong.ServiceResponse.GetRawBody()
	if err != nil {
		kong.Log.Warn(err)
	}

	// Gets the response status
	statusCode, err := kong.ServiceResponse.GetStatus()
	if err != nil {
		kong.Log.Warn(err)
	}

	// Gets all the headers
	headers, err := kong.Response.GetHeaders(99)
	if err != nil {
		kong.Log.Warn(err)
	}

	response := make(map[string]interface{})

	response["path"] = path
	response["statusCode"] = statusCode
	response["body"] = body
	response["headers"] = headers

	raw := responseJsonMessage{
		Response: response,
		TraceId: traceId,
		Date: timestamp,
	}

	go sendMessage(kong, raw, conf.Topic, conf.KafkaBrokers)
}

// Kafka producer
// It sends the JSON (responseJsonMessage or requestJsonMessage) as raw message body
func sendMessage(kong *pdk.PDK, message interface{}, topic string, kafkaBrokers string) {
	json, err := json.Marshal(message)

	if (err != nil) {
		kong.Log.Err(err)
		return
	}

	w := &kafka.Writer{
		Addr: kafka.TCP(strings.Split(kafkaBrokers, ",")...),
		Topic:   topic,
		Balancer: &kafka.LeastBytes{},
	}

	err = w.WriteMessages(context.Background(), 
	kafka.Message{
		Key: []byte(nil),
		Value: []byte(json),
	})
	if err != nil {
		kong.Log.Err("could not write message " + err.Error())
		return
	}
}
