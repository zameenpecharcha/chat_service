package logger

import (
    "os"
    "strings"

    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

func New(level string) zerolog.Logger {
    zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
    lvl, err := zerolog.ParseLevel(strings.ToLower(level))
    if err != nil { lvl = zerolog.InfoLevel }
    l := log.Output(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
        w.Out = os.Stdout
    })).Level(lvl)
    return l
}


