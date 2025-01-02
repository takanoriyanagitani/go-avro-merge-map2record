package merge

import (
	"context"
	"iter"

	mr "github.com/takanoriyanagitani/go-avro-merge-map2record"
	. "github.com/takanoriyanagitani/go-avro-merge-map2record/util"
)

type OriginalToMerged func(mr.OriginalRecord) IO[mr.MergedRecord]

type MapColumnName string

func (m MapColumnName) ToOriginalToMerged() OriginalToMerged {
	buf := mr.MergedRecord{}
	return func(original mr.OriginalRecord) IO[mr.MergedRecord] {
		return func(_ context.Context) (mr.MergedRecord, error) {
			clear(buf)

			for key, val := range original {
				if string(m) != key {
					buf[key] = val
				}
			}

			var mapField map[string]any
			var a any = original[string(m)]
			switch t := a.(type) {
			case map[string]any:
				mapField = t
			default:
			}

			if nil != mapField {
				for key, val := range mapField {
					buf[key] = val
				}
			}

			return buf, nil
		}
	}
}

func (o OriginalToMerged) MapsToMaps(
	m iter.Seq2[map[string]any, error],
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			for original, e := range m {
				if nil != e {
					yield(map[string]any{}, e)
					return
				}

				merged, e := o(original)(ctx)

				if !yield(merged, e) {
					return
				}
			}
		}, nil
	}
}
