package output

import (
	"context"
	"deinopis/internal/model"
)

// OutputSink represents a destination for normalized DEINOPIS events.
type OutputSink interface {
	Emit(ctx context.Context, event *model.NormalizedEvent) error
	Flush() error
	Close() error
}
