package reads_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	influxdb2 "github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/readservice"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestTable_ReadFilter(t *testing.T) {
	engine := NewDefaultEngine(t)
	engine.MustOpen()
	defer engine.Close()

	input := `
cpu,host=server01,region=useast2 value=1i 0
cpu,host=server02,region=useast2 value=7i 70
`

	mm := tsdb.EncodeName(engine.org, engine.bucket)
	points, err := models.ParsePoints([]byte(input), mm[:])
	if err != nil {
		t.Fatalf("error parsing points: %s", err)
	}

	if err := engine.WritePoints(context.Background(), points); err != nil {
		t.Fatalf("error writing points to engine: %s", err)
	}

	s := reads.NewReader(readservice.NewStore(engine.Engine))
	executetest.RunTableTests(t, executetest.TableTest{
		NewFn: func(ctx context.Context, alloc *memory.Allocator) flux.TableIterator {
			spec := influxdb2.ReadFilterSpec{
				OrganizationID: engine.org,
				BucketID:       engine.bucket,
				Bounds: execute.Bounds{
					Start: 0,
					Stop:  execute.Time(60),
				},
			}
			tables, err := s.ReadFilter(ctx, spec, alloc)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			return tables
		},
		IsDone: reads.IsDone,
	})
}

func TestTable_ReadGroup(t *testing.T) {
	engine := NewDefaultEngine(t)
	engine.MustOpen()
	defer engine.Close()

	input := `
cpu,host=server01,region=useast2 value=1i 0
cpu,host=server02,region=useast2 value=7i 70
`

	mm := tsdb.EncodeName(engine.org, engine.bucket)
	points, err := models.ParsePoints([]byte(input), mm[:])
	if err != nil {
		t.Fatalf("error parsing points: %s", err)
	}

	if err := engine.WritePoints(context.Background(), points); err != nil {
		t.Fatalf("error writing points to engine: %s", err)
	}

	s := reads.NewReader(readservice.NewStore(engine.Engine))
	executetest.RunTableTests(t, executetest.TableTest{
		NewFn: func(ctx context.Context, alloc *memory.Allocator) flux.TableIterator {
			spec := influxdb2.ReadGroupSpec{
				ReadFilterSpec: influxdb2.ReadFilterSpec{
					OrganizationID: engine.org,
					BucketID:       engine.bucket,
					Bounds: execute.Bounds{
						Start: 0,
						Stop:  execute.Time(60),
					},
				},
				GroupMode: influxdb2.GroupModeBy,
				GroupKeys: []string{"host"},
			}
			tables, err := s.ReadGroup(ctx, spec, alloc)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			return tables
		},
		IsDone: reads.IsDone,
	})
}

type Engine struct {
	t           *testing.T
	path        string
	org, bucket influxdb.ID
	logger      *zap.Logger

	*storage.Engine
}

// NewEngine create a new wrapper around a storage engine.
func NewEngine(t *testing.T, c storage.Config) *Engine {
	logger := zaptest.NewLogger(t)
	path, _ := ioutil.TempDir("", "storage_engine_test")

	engine := storage.NewEngine(path, c)
	engine.WithLogger(logger)

	org, err := influxdb.IDFromString("3131313131313131")
	if err != nil {
		panic(err)
	}

	bucket, err := influxdb.IDFromString("3232323232323232")
	if err != nil {
		panic(err)
	}

	return &Engine{
		path:   path,
		org:    *org,
		bucket: *bucket,
		logger: logger,
		Engine: engine,
	}
}

// NewDefaultEngine returns a new Engine with a default configuration.
func NewDefaultEngine(t *testing.T) *Engine {
	config := storage.NewConfig()
	return NewEngine(t, config)
}

// MustOpen opens the engine or panics.
func (e *Engine) MustOpen() {
	if err := e.Engine.Open(context.Background()); err != nil {
		e.t.Fatalf("error opening engine: %s", err)
	}
}

// Close closes the engine and removes all temporary data.
func (e *Engine) Close() {
	if err := e.Engine.Close(); err != nil {
		e.t.Errorf("error closing engine: %s", err)
	}
	if err := os.RemoveAll(e.path); err != nil {
		e.t.Errorf("could not remove directory %s: %s", e.path, err)
	}
}
