package storage

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/localization"
)

type compositeReadEvent[Row any] struct {
	row        Row
	finished   bool
	err        error
	panicValue any
}

// A reservation holds the provider inside its callback until the consumer
// advances. Its row and read dependencies remain valid without copying or
// buffering the stream. There is one reservation per constituent.
type compositeReadReservation[Row any] struct {
	events   chan compositeReadEvent[Row]
	advance  chan struct{}
	stop     chan struct{}
	finished chan struct{}
}

func reserveCompositeRead[T, Row any](engine Engine, read func(T, func(Row) error) error, fallback func(func(Row) error) error) *compositeReadReservation[Row] {
	r := &compositeReadReservation[Row]{
		events:   make(chan compositeReadEvent[Row]),
		advance:  make(chan struct{}),
		stop:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	go func() {
		defer func() {
			if value := recover(); value != nil {
				select {
				case r.events <- compositeReadEvent[Row]{finished: true, panicValue: value}:
				case <-r.stop:
				}
			}
			close(r.finished)
		}()
		err := VisitWithCapability(engine, read, fallback, func(row Row) error {
			select {
			case r.events <- compositeReadEvent[Row]{row: row}:
			case <-r.stop:
				return ErrIterationStopped
			}
			select {
			case <-r.advance:
				return nil
			case <-r.stop:
				return ErrIterationStopped
			}
		})
		select {
		case r.events <- compositeReadEvent[Row]{finished: true, err: err}:
		case <-r.stop:
		}
	}()
	return r
}

// visitCompositeRead dispatches a total ordinary read sequentially, choosing
// each child's fallback before that child delivers. Without an ordinary fallback,
// it admits all constituents before external delivery using held reservations. A first row holds its running
// provider; a completed empty read needs no held row. A refusal with no fallback
// releases every provider without delivery. No constituent is replayed, and an
// early stop releases and joins all reservations. Pending rows and bookkeeping
// scale with constituents, not entities from capable providers.
func visitCompositeRead[T, Row any](ctx context.Context, c *CompositeEngine, read func(T, string, Engine, func(Row) error) error, fallback func(string, Engine, func(Row) error) error, visit func(Row) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	aliases, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return err
	}
	if fallback != nil {
		for _, alias := range aliases {
			if err := ctx.Err(); err != nil {
				return err
			}
			engine, err := c.getConstituent(alias)
			if err != nil {
				return err
			}
			if err := VisitWithCapability(engine, func(reader T, deliver func(Row) error) error {
				return compositeStreamError(alias, read(reader, alias, engine, deliver))
			}, func(deliver func(Row) error) error {
				if err := fallback(alias, engine, deliver); err != nil {
					return localizedError(localization.StorageCompositeConstituentQueryFailed(alias, err), err)
				}
				return nil
			}, visit); err != nil {
				return err
			}
		}
		return nil
	}
	reservations := make([]*compositeReadReservation[Row], 0, len(aliases))
	first := make([]compositeReadEvent[Row], 0, len(aliases))
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		for _, r := range reservations {
			close(r.stop)
		}
		for _, r := range reservations {
			<-r.finished
		}
	}
	defer release()
	for _, alias := range aliases {
		if err := ctx.Err(); err != nil {
			return err
		}
		engine, err := c.getConstituent(alias)
		if err != nil {
			return err
		}
		r := reserveCompositeRead(engine, func(reader T, deliver func(Row) error) error {
			return compositeStreamError(alias, read(reader, alias, engine, deliver))
		}, nil)
		reservations = append(reservations, r)
		event := <-r.events
		if event.panicValue != nil {
			panic(event.panicValue)
		}
		if event.finished && event.err != nil {
			return event.err
		}
		first = append(first, event)
	}
	for i, r := range reservations {
		event := first[i]
		for !event.finished {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(event.row); err != nil {
				return err
			}
			r.advance <- struct{}{}
			event = <-r.events
			if event.panicValue != nil {
				panic(event.panicValue)
			}
		}
		if event.err != nil {
			return event.err
		}
	}
	return nil
}
