// Package sse implements a k6/x/sse javascript module extension for k6.
// It provides basic functionality to handle Server-Sent Event over http
// that *blocks* the event loop while the http connection is opened.
// [SSE API design document]:
// https://github.com/phymbert/xk6-sse/blob/master/docs/design/021-sse-api.md#proposed-solution
package sse

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	httpModule "go.k6.io/k6/js/modules/k6/http"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/metrics"
)

type (
	// sse represents a module instance of the sse module.
	sse struct {
		vu      modules.VU
		obj     *sobek.Object
		metrics *sseMetrics
	}
)

// ErrSSEInInitContext is returned when sse are using in the init context
var ErrSSEInInitContext = common.NewInitContextError("using sse in the init context is not supported")

// Client is the representation of the sse returned to the js.
type Client struct {
	rt            *sobek.Runtime
	ctx           context.Context
	url           string
	resp          *http.Response
	eventHandlers map[string][]sobek.Callable
	done          chan struct{}
	shutdownOnce  sync.Once

	tagsAndMeta    *metrics.TagsAndMeta
	samplesOutput  chan<- metrics.SampleContainer
	builtinMetrics *metrics.BuiltinMetrics
	sseMetrics     *sseMetrics
	cancelRequest  context.CancelFunc
	httpClient     *http.Client
}

// HTTPResponse is the http response returned by sse.open.
type HTTPResponse struct {
	URL     string            `json:"url"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Error   string            `json:"error"`
	Body    string            `json:"body"`
}

// Event represents a Server-Sent Event
type Event struct {
	ID      string
	Comment string
	Name    string
	Data    string
}

type sseOpenArgs struct {
	setupFn      sobek.Callable
	headers      http.Header
	method       string
	body         string
	cookieJar    *cookiejar.Jar
	tagsAndMeta  *metrics.TagsAndMeta
	timeout      time.Duration
	streamFormat string
}

// Exports returns the exports of the sse module.
func (mi *sse) Exports() modules.Exports {
	return modules.Exports{Default: mi.obj}
}

// Open establishes a http client connection based on the parameters provided.
func (mi *sse) Open(url string, args ...sobek.Value) (*HTTPResponse, error) {
	ctx := mi.vu.Context()
	rt := mi.vu.Runtime()
	state := mi.vu.State()
	if state == nil {
		return nil, ErrSSEInInitContext
	}

	parsedArgs, err := parseConnectArgs(state, rt, args...)
	if err != nil {
		return nil, err
	}

	parsedArgs.tagsAndMeta.SetSystemTagOrMetaIfEnabled(state.Options.SystemTags, metrics.TagURL, url)

	client, connEndHook, err := mi.open(ctx, state, rt, url, parsedArgs)
	defer connEndHook()
	if err != nil {
		// Pass the error to the user script before exiting immediately
		client.handleEvent("error", rt.ToValue(err))
		if state.Options.Throw.Bool {
			return nil, err
		}
		return client.wrapHTTPResponse(err.Error()), nil
	}

	if !strings.Contains(client.resp.Header.Get("Content-Type"), "text/event-stream") &&
		parsedArgs.streamFormat != "bedrock" {
		// Non-SSE response, wrap it and return immediately
		return client.wrapHTTPResponse(""), nil
	}

	// Run the user-provided set up function
	if _, err := parsedArgs.setupFn(sobek.Undefined(), rt.ToValue(&client)); err != nil {
		_ = client.closeResponseBody()
		return nil, err
	}

	// The connection is now open, emit the event
	client.handleEvent("open")

	readEventChan := make(chan Event)
	readErrChan := make(chan error)
	readCloseChan := make(chan int)

	// Choose parser based on streamFormat
	if parsedArgs.streamFormat == "bedrock" {
		go client.readBedrockEvents(readEventChan, readErrChan, readCloseChan)
	} else {
		go client.readEvents(readEventChan, readErrChan, readCloseChan)
	}

	// Main event loop
	for {
		select {
		case event := <-readEventChan:
			metrics.PushIfNotDone(ctx, client.samplesOutput, metrics.Sample{
				TimeSeries: metrics.TimeSeries{
					Metric: client.sseMetrics.SSEEventReceived,
					Tags:   client.tagsAndMeta.Tags,
				},
				Time:     time.Now(),
				Metadata: client.tagsAndMeta.Metadata,
				Value:    1,
			})

			client.handleEvent("event", rt.ToValue(event))

		case readErr := <-readErrChan:
			client.handleEvent("error", rt.ToValue(readErr))

		case <-ctx.Done():
			// VU is shutting down during an interrupt
			// client events will not be forwarded to the VU
			_ = client.closeResponseBody()

		case <-readCloseChan:
			_ = client.closeResponseBody()

		case <-client.done:
			// This is the final exit point normally triggered by closeResponseBody
			return client.wrapHTTPResponse(""), nil
		}
	}
}

func (mi *sse) open(ctx context.Context, state *lib.State, rt *sobek.Runtime,
	url string, args *sseOpenArgs,
) (*Client, func(), error) {
	reqCtx, cancel := context.WithCancel(ctx)

	sseClient := Client{
		ctx:            ctx,
		rt:             rt,
		url:            url,
		eventHandlers:  make(map[string][]sobek.Callable),
		done:           make(chan struct{}),
		samplesOutput:  state.Samples,
		tagsAndMeta:    args.tagsAndMeta,
		builtinMetrics: state.BuiltinMetrics,
		sseMetrics:     mi.metrics,
		cancelRequest:  cancel,
	}

	// Overriding the NextProtos to avoid talking http2
	var tlsConfig *tls.Config
	if state.TLSConfig != nil {
		tlsConfig = state.TLSConfig.Clone()
		tlsConfig.NextProtos = []string{"http/1.1"}
	}

	sseClient.httpClient = &http.Client{
		// FUTURE: support falling back on global timeout re: https://github.com/grafana/k6/issues/3932
		Timeout: args.timeout,
		Transport: &http.Transport{
			DialContext:     state.Dialer.DialContext,
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
			// FIXME phymbert: it would be more interesting to allow reusing the transport across iterations
			DisableKeepAlives: state.Options.NoConnectionReuse.ValueOrZero() || state.Options.NoVUConnectionReuse.ValueOrZero(),
		},
	}

	// httpClient.Jar must never be nil
	if args.cookieJar != nil {
		sseClient.httpClient.Jar = args.cookieJar
	}

	httpMethod := http.MethodGet
	if args.method != "" {
		httpMethod = args.method
	}

	req, err := http.NewRequestWithContext(reqCtx, httpMethod, url, strings.NewReader(args.body))
	if err != nil {
		return &sseClient, nil, err
	}

	req.Header.Set("Accept", "text/event-stream")
	for headerName, headerValues := range args.headers {
		for _, headerValue := range headerValues {
			req.Header.Set(headerName, headerValue)
		}
	}

	// Wrap the request to retrieve the server IP tag
	trace := &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			if state.Options.SystemTags.Has(metrics.TagIP) {
				if ip, _, err2 := net.SplitHostPort(connInfo.Conn.RemoteAddr().String()); err2 == nil {
					args.tagsAndMeta.SetSystemTagOrMeta(metrics.TagIP, ip)
				}
			}
		},
	}

	//nolint:contextcheck // parent context already passed in the request
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	connStart := time.Now()
	//nolint:bodyclose // Body is deferred closed in closeResponseBody
	resp, err := sseClient.httpClient.Do(req)
	connEnd := time.Now()

	if resp != nil {
		sseClient.resp = resp
		if state.Options.SystemTags.Has(metrics.TagStatus) {
			args.tagsAndMeta.SetSystemTagOrMeta(
				metrics.TagStatus, strconv.Itoa(resp.StatusCode))
		}
	}

	connEndHook := sseClient.pushSSEMetrics(connStart, connEnd)

	return &sseClient, connEndHook, err
}

// On is used to configure what the client should do on each event.
func (c *Client) On(event string, handler sobek.Value) {
	if handler, ok := sobek.AssertFunction(handler); ok {
		c.eventHandlers[event] = append(c.eventHandlers[event], handler)
	}
}

// Close the event loop
func (c *Client) Close() error {
	err := c.closeResponseBody()
	c.cancelRequest()
	c.httpClient.CloseIdleConnections()
	return err
}

func (c *Client) handleEvent(event string, args ...sobek.Value) {
	if handlers, ok := c.eventHandlers[event]; ok {
		for _, handler := range handlers {
			if _, err := handler(sobek.Undefined(), args...); err != nil {
				common.Throw(c.rt, err)
			}
		}
	}
}

// closeResponseBody cleanly closes the response body.
// Returns an error if sending the response body cannot be closed.
func (c *Client) closeResponseBody() error {
	var err error

	c.shutdownOnce.Do(func() {
		err = c.resp.Body.Close()
		if err != nil {
			c.handleEvent("error", c.rt.ToValue(err))
		}
		close(c.done)
	})

	return err
}

func (c *Client) pushSSEMetrics(connStart, connEnd time.Time) func() {
	connDuration := metrics.D(connEnd.Sub(connStart))

	metrics.PushIfNotDone(c.ctx, c.samplesOutput, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				TimeSeries: metrics.TimeSeries{
					Metric: c.builtinMetrics.HTTPReqSending,
					Tags:   c.tagsAndMeta.Tags,
				},
				Time:     connStart,
				Metadata: c.tagsAndMeta.Metadata,
				Value:    connDuration,
			},
		},
		Tags: c.tagsAndMeta.Tags,
		Time: connStart,
	})

	return func() {
		end := time.Now()
		requestDuration := metrics.D(end.Sub(connStart))

		metrics.PushIfNotDone(c.ctx, c.samplesOutput, metrics.ConnectedSamples{
			Samples: []metrics.Sample{
				{
					TimeSeries: metrics.TimeSeries{
						Metric: c.builtinMetrics.HTTPReqs,
						Tags:   c.tagsAndMeta.Tags,
					},
					Time:     end,
					Metadata: c.tagsAndMeta.Metadata,
					Value:    1,
				},
				{
					TimeSeries: metrics.TimeSeries{
						Metric: c.builtinMetrics.HTTPReqSending,
						Tags:   c.tagsAndMeta.Tags,
					},
					Time:     end,
					Metadata: c.tagsAndMeta.Metadata,
					Value:    connDuration,
				},
				{
					TimeSeries: metrics.TimeSeries{
						Metric: c.builtinMetrics.HTTPReqDuration,
						Tags:   c.tagsAndMeta.Tags,
					},
					Time:     end,
					Metadata: c.tagsAndMeta.Metadata,
					Value:    requestDuration,
				},
			},
			Tags: c.tagsAndMeta.Tags,
			Time: end,
		})
	}
}

// Wraps SSE in a channel, follow the SSE format described in:
// https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
func (c *Client) readEvents(readChan chan Event, errorChan chan error, closeChan chan int) {
	reader := bufio.NewReader(c.resp.Body)
	ev := Event{}
	var buf bytes.Buffer

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				select {
				case closeChan <- -1:
					return
				case <-c.done:
					return
				}
			} else {
				select {
				case errorChan <- err:
					return
				case <-c.done:
					return
				}
			}
		}

		switch {
		// id of event
		case hasPrefix(line, "id: "):
			ev.ID = stripPrefix(line, 4)
		case hasPrefix(line, "id:"):
			ev.ID = stripPrefix(line, 3)

		// Comment
		case hasPrefix(line, ": "):
			ev.Comment = stripPrefix(line, 2)
		case hasPrefix(line, ":"):
			ev.Comment = stripPrefix(line, 1)

		// name of event
		case hasPrefix(line, "event: "):
			ev.Name = stripPrefix(line, 7)
		case hasPrefix(line, "event:"):
			ev.Name = stripPrefix(line, 6)

		// event data
		case hasPrefix(line, "data: "):
			buf.Write(line[6:])

		case hasPrefix(line, "data:"):
			buf.Write(line[5:])

		case hasPrefix(line, "retry:"):
			// Retry, do nothing for now

		// end of event
		case isLineEnd(line):
			// Trailing newlines are removed.
			ev.Data = strings.TrimRightFunc(buf.String(), func(r rune) bool {
				return r == '\r' || r == '\n'
			})

			select {
			case readChan <- ev:
				buf.Reset()
				ev = Event{}
			case <-c.done:
				return
			}
		default:
			select {
			case errorChan <- errors.New("unknown event: " + string(line)):
			case <-c.done:
				return
			}
		}
	}
}

// Handle AWS Bedrock's binary event stream format
func (c *Client) readBedrockEvents(readChan chan Event, errorChan chan error, closeChan chan int) {
	reader := bufio.NewReader(c.resp.Body)
	var buffer bytes.Buffer

	for {
		// Read more data into buffer
		chunk := make([]byte, 4096)
		n, err := reader.Read(chunk)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if buffer.Len() > 0 {
					c.processBedrockBuffer(buffer.Bytes(), readChan)
				}
				select {
				case closeChan <- -1:
					return
				case <-c.done:
					return
				}
			} else {
				select {
				case errorChan <- err:
					return
				case <-c.done:
					return
				}
			}
		}

		buffer.Write(chunk[:n])
		remaining := c.processBedrockBuffer(buffer.Bytes(), readChan)
		buffer.Reset()
		if len(remaining) > 0 {
			buffer.Write(remaining)
		}
	}
}

func (c *Client) processBedrockBuffer(data []byte, readChan chan Event) []byte {
	dataStr := string(data)
	events, remaining := c.findBedrockEvents(dataStr)

	for _, eventData := range events {
		if ev := c.parseBedrockEvent(eventData); ev != nil {
			select {
			case readChan <- *ev:
			case <-c.done:
				return []byte(remaining)
			}
		}
	}

	return []byte(remaining)
}

func (c *Client) findBedrockEvents(data string) ([]string, string) {
	eventMarker := ":event-type"
	var events []string
	var indices []int

	// Find all event markers
	for pos := 0; pos < len(data); {
		if idx := strings.Index(data[pos:], eventMarker); idx != -1 {
			indices = append(indices, pos+idx)
			pos += idx + len(eventMarker)
		} else {
			break
		}
	}

	if len(indices) == 0 {
		return nil, data
	}

	// Extract complete events
	for i := 0; i < len(indices)-1; i++ {
		eventData := data[indices[i]:indices[i+1]]
		if strings.TrimSpace(eventData) != "" {
			events = append(events, eventData)
		}
	}

	// Handle the last event
	lastEventData := data[indices[len(indices)-1]:]
	if c.isBedrockEventComplete(lastEventData) {
		events = append(events, lastEventData)
		return events, ""
	}

	return events, lastEventData
}

func (c *Client) isBedrockEventComplete(eventData string) bool {
	cleanData := cleanNonPrintable(eventData)
	if jsonStart := strings.Index(cleanData, "{"); jsonStart != -1 {
		return extractCompleteJSON(cleanData[jsonStart:]) != ""
	}
	return false
}

func (c *Client) parseBedrockEvent(eventData string) *Event {
	cleanData := cleanNonPrintable(eventData)
	cleanData = strings.Join(strings.Fields(cleanData), " ")

	// Extract JSON
	jsonStart := strings.Index(cleanData, "{")
	if jsonStart == -1 {
		return nil
	}

	jsonData := extractCompleteJSON(cleanData[jsonStart:])
	if jsonData == "" {
		return nil
	}

	// Validate JSON
	var jsonObj map[string]interface{}
	if err := json.Unmarshal([]byte(jsonData), &jsonObj); err != nil {
		return nil
	}

	// Determine event type
	eventType := determineBedrockEventType(cleanData, jsonObj)

	return &Event{
		Name: eventType,
		Data: jsonData,
	}
}

// Helper functions
func cleanNonPrintable(data string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 32 && r <= 126) || r >= 0x80 {
			return r
		}
		return ' '
	}, data)
}

func extractCompleteJSON(data string) string {
	braceCount := 0
	inString := false
	escaped := false

	for i, char := range data {
		if escaped {
			escaped = false
			continue
		}

		switch char {
		case '\\':
			escaped = true
		case '"':
			inString = !inString
		case '{':
			if !inString {
				braceCount++
			}
		case '}':
			if !inString {
				braceCount--
				if braceCount == 0 {
					return data[:i+1]
				}
			}
		}
	}

	return ""
}

func determineBedrockEventType(cleanData string, jsonObj map[string]interface{}) string {
	// First try to determine from the event data itself
	eventTypes := []string{"contentBlockDelta", "messageStart", "messageStop", "contentBlockStop", "metadata"}
	for _, eventType := range eventTypes {
		if strings.Contains(cleanData, eventType) {
			return eventType
		}
	}

	// Fallback to JSON content analysis
	if _, ok := jsonObj["delta"]; ok {
		return "contentBlockDelta"
	}
	if _, ok := jsonObj["role"]; ok {
		return "messageStart"
	}
	if _, ok := jsonObj["stopReason"]; ok {
		return "messageStop"
	}
	if _, ok := jsonObj["metrics"]; ok {
		return "metadata"
	}

	return "message"
}

func (c *Client) wrapHTTPResponse(errMessage string) *HTTPResponse {
	if errMessage != "" {
		return &HTTPResponse{Error: errMessage}
	}

	if c.resp == nil {
		return &HTTPResponse{Error: "No HTTP response available"}
	}

	headers := make(map[string]string, len(c.resp.Header))
	for k, vs := range c.resp.Header {
		headers[k] = strings.Join(vs, ", ")
	}

	// Default response without body
	resp := &HTTPResponse{
		URL:     c.url,
		Status:  c.resp.StatusCode,
		Headers: headers,
	}

	// If this isn't an SSE stream, try to read and include the body for diagnostics.
	// We consider it non-SSE when Content-Type doesn't include text/event-stream.
	contentType := c.resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/event-stream") {
		fmt.Println("Content-Type is not text/event-stream")
		// Read up to a reasonable cap to avoid unbounded memory usage.
		// 256KB should be sufficient for typical error payloads.
		const maxBody = 256 * 1024
		var buf bytes.Buffer
		limited := io.LimitedReader{R: c.resp.Body, N: maxBody + 1}
		_, _ = io.Copy(&buf, &limited)

		// Close since we're consuming the body here
		_ = c.closeResponseBody()

		body := buf.Bytes()
		if len(body) > maxBody {
			body = body[:maxBody]
		}
		resp.Body = string(body)
	}

	return resp
}

func parseConnectArgs(state *lib.State, rt *sobek.Runtime, args ...sobek.Value) (*sseOpenArgs, error) {
	var callableV, paramsV sobek.Value
	switch len(args) {
	case 2:
		paramsV = args[0]
		callableV = args[1]
	case 1:
		paramsV = sobek.Undefined()
		callableV = args[0]
	default:
		return nil, errors.New("invalid number of arguments to sse.open")
	}

	setupFn, isFunc := sobek.AssertFunction(callableV)
	if !isFunc {
		return nil, errors.New("last argument to sse.open must be a function")
	}

	headers := make(http.Header)
	headers.Set("User-Agent", state.Options.UserAgent.String)
	tagsAndMeta := state.Tags.GetCurrentValues()
	parsedArgs := &sseOpenArgs{
		setupFn:      setupFn,
		headers:      headers,
		cookieJar:    state.CookieJar,
		tagsAndMeta:  &tagsAndMeta,
		timeout:      0,
		streamFormat: "sse",
	}

	if sobek.IsUndefined(paramsV) || sobek.IsNull(paramsV) {
		return parsedArgs, nil
	}

	err := parseConnectOptionalArgs(paramsV, rt, parsedArgs)
	if err != nil {
		return nil, err
	}

	return parsedArgs, nil
}

func parseConnectOptionalArgs(paramsV sobek.Value, rt *sobek.Runtime, parsedArgs *sseOpenArgs) error {
	params := paramsV.ToObject(rt)
	for _, k := range params.Keys() {
		switch k {
		case "headers":
			if headersV := params.Get(k); !sobek.IsUndefined(headersV) && !sobek.IsNull(headersV) {
				if headersObj := headersV.ToObject(rt); headersObj != nil {
					for _, key := range headersObj.Keys() {
						parsedArgs.headers.Set(key, headersObj.Get(key).String())
					}
				}
			}
		case "tags":
			if err := common.ApplyCustomUserTags(rt, parsedArgs.tagsAndMeta, params.Get(k)); err != nil {
				return fmt.Errorf("invalid sse.open() metric tags: %w", err)
			}
		case "jar":
			if jarV := params.Get(k); !sobek.IsUndefined(jarV) && !sobek.IsNull(jarV) {
				if v, ok := jarV.Export().(*httpModule.CookieJar); ok {
					parsedArgs.cookieJar = v.Jar
				}
			}
		case "method":
			parsedArgs.method = strings.TrimSpace(params.Get(k).ToString().String())
		case "body":
			parsedArgs.body = strings.TrimSpace(params.Get(k).ToString().String())
		case "timeout":
			timeoutV := params.Get(k)
			if sobek.IsUndefined(timeoutV) || sobek.IsNull(timeoutV) {
				continue
			}
			timeout, err := time.ParseDuration(timeoutV.ToString().String())
			if err != nil {
				return fmt.Errorf("invalid sse.open() timeout: %w", err)
			}
			parsedArgs.timeout = timeout
		case "streamFormat":
			if format := params.Get(k).String(); format == "bedrock" {
				parsedArgs.streamFormat = format
			}
		}
	}
	return nil
}

func hasPrefix(s []byte, prefix string) bool {
	return bytes.HasPrefix(s, []byte(prefix))
}

func stripPrefix(line []byte, start int) string {
	return string(line[start : len(line)-1])
}

func isLineEnd(line []byte) bool {
	return bytes.Equal(line, []byte("\n")) || bytes.Equal(line, []byte("\r\n"))
}
