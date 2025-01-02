package enc

import (
	"context"
	"log"
	"fmt"
	"errors"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	mr "github.com/takanoriyanagitani/go-avro-merge-map2record"
	. "github.com/takanoriyanagitani/go-avro-merge-map2record/util"
)

var (
	ErrInvalidInput error = errors.New("invalid input")
	ErrEncode       error = errors.New("unable to encode")
	ErrFlush        error = errors.New("unable to flush")
)

func MapsToWriterHamba(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, e := ho.NewEncoderWithSchema(
		s,
		w,
		opts...,
	)
	if nil != e {
		return e
	}
	defer enc.Close()

	for row, e := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != e {
			return errors.Join(ErrInvalidInput, e)
		}

		e = enc.Encode(row)
		if nil != e {
			for key, val := range row {
				log.Printf("rejected key/val: %s/%v\n", key, val)
			}
			return errors.Join(
				fmt.Errorf("%w: key/val pairs count: %v", ErrEncode, len(row)),
				e,
			)
		}

		e = enc.Flush()
		if nil != e {
			return errors.Join(ErrFlush, e)
		}
	}

	return enc.Flush()
}

func CodecConv(c mr.Codec) ho.CodecName {
	switch c {
	case mr.CodecNull:
		return ho.Null
	case mr.CodecDeflate:
		return ho.Deflate
	case mr.CodecSnappy:
		return ho.Snappy
	case mr.CodecZstd:
		return ho.ZStandard
	default:
		return ho.Null
	}
}

func ConfigToOpts(cfg mr.EncodeConfig) []ho.EncoderFunc {
	var c ho.CodecName = CodecConv(cfg.Codec)
	return []ho.EncoderFunc{
		ho.WithBlockLength(cfg.BlockLength),
		ho.WithCodec(c),
	}
}

func MapsToWriter(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	schema string,
	cfg mr.EncodeConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}

	var opts []ho.EncoderFunc = ConfigToOpts(cfg)
	return MapsToWriterHamba(
		ctx,
		m,
		w,
		parsed,
		opts...,
	)
}

func MapsToStdout(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
	cfg mr.EncodeConfig,
) error {
	return MapsToWriter(
		ctx,
		m,
		os.Stdout,
		schema,
		cfg,
	)
}

func MapsToStdoutDefault(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
) error {
	return MapsToStdout(ctx, m, schema, mr.EncodeConfigDefault)
}

func SchemaToMapsToStdoutDefault(
	schema string,
) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return func(m iter.Seq2[map[string]any, error]) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			return Empty, MapsToStdoutDefault(
				ctx,
				m,
				schema,
			)
		}
	}
}
