package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-merge-map2record/util"

	rm "github.com/takanoriyanagitani/go-avro-merge-map2record/merge"

	dh "github.com/takanoriyanagitani/go-avro-merge-map2record/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-merge-map2record/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var mapColName IO[rm.MapColumnName] = Bind(
	EnvValByKey("ENV_MAP_COLNAME"),
	Lift(func(s string) (rm.MapColumnName, error) {
		return rm.MapColumnName(s), nil
	}),
)

var otm IO[rm.OriginalToMerged] = Bind(
	mapColName,
	Lift(func(mcn rm.MapColumnName) (rm.OriginalToMerged, error) {
		return mcn.ToOriginalToMerged(), nil
	}),
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	otm,
	func(o2m rm.OriginalToMerged) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			func(
				i iter.Seq2[map[string]any, error],
			) IO[iter.Seq2[map[string]any, error]] {
				return o2m.MapsToMaps(i)
			},
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}
		defer f.Close()

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
