package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

const (
	// Jetstream
	SCHEME                 string = "wss"
	DEFAULT_JETSTREAM_HOST string = "jetstream2.us-east.bsky.network"
	SUBSCRIBE              string = "/subscribe"
	// Data
	DATA_DIR string = "data/"
	// Buffers(ish)
	DEFAULT_PURGE_AFTER int = 10000
)

type Bluestream struct {
	host       string
	conn       *websocket.Conn
	purgeAfter int
	// skeets chan []interface{} // TODO -> do this async with goroutines
}

func ensureDir(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

func purgeToFile(skeets []interface{}) {
	log.Info().Msg("purging skeets to file")
	now := time.Now().UTC().Format(time.RFC3339)
	filename := DATA_DIR + now + "_skeets.json.gz"
	file, err := os.Create(filename)
	if err != nil {
		log.Error().Err(err).Msg("could not create file")
	}
	gz, err := gzip.NewWriterLevel(file, gzip.BestCompression)
	if err != nil {
		log.Error().Err(err).Msg("could not create gz writer")
	}
	for _, skeet := range skeets {
		enc := json.NewEncoder(gz)
		err = enc.Encode(skeet)
		if err != nil {
			log.Error().Err(err).Msg("could not encode skeet")
			continue
		}
		_, err = gz.Write([]byte("\n"))
		if err != nil {
			log.Error().Err(err).Msg("could not write new line")
			continue
		}
	}
	gz.Close()
	file.Close()
}

func (b *Bluestream) initialize() {
	// Set config and whatnot
	b.host = DEFAULT_JETSTREAM_HOST // Get from env or something
	b.purgeAfter = DEFAULT_PURGE_AFTER

	// Dial websocket
	url := url.URL{Scheme: SCHEME, Host: b.host, Path: SUBSCRIBE}

	conn, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Fatal().Err(err).Msg("could not dial websocket")
	}
	b.conn = conn

	// Ensure data dir exists
	ensureDir(DATA_DIR)
}

func (b *Bluestream) run() {

	i := 0
	skeets := make([]interface{}, 0)
	for {
		_, message, err := b.conn.ReadMessage()
		if err != nil {
			log.Error().Err(err).Msg("could not read message")
		}
		var data interface{}
		err = json.Unmarshal(message, &data)
		if err != nil {
			log.Error().Err(err).Msg("could not unmarshal message")
			continue
		}
		skeets = append(skeets, data)
		i++
		if i == b.purgeAfter {
			purgeToFile(skeets)
			skeets = make([]interface{}, 0)
			i = 0
		}
	}
}

func (b *Bluestream) shutdown() {
	b.conn.Close()
	log.Info().Msg("shut down")
}

func main() {
	stream := Bluestream{}
	stream.initialize()
	stream.run()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutting down skeetstream...")
	stream.shutdown()
	_, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
}