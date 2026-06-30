package provider

import (
	"context"
	"time"

	"github.com/example/earthquake-service/internal/domain/earthquake"
)

type FetchMetadata struct {
	ETag, LastModified string
	NotModified        bool
	InvalidCount       int
}

type CacheValidators struct{ ETag, LastModified string }

type Provider interface {
	Name() string
	FetchRealtime(context.Context, CacheValidators) ([]earthquake.Event, FetchMetadata, error)
	FetchHistorical(context.Context, time.Time, time.Time, *string) ([]earthquake.Event, *string, FetchMetadata, error)
}
