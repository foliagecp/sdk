package statefun

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ShutdownContext_ResetCancelsPreviousGeneration(t *testing.T) {
	gs := NewGracefulShutdown(context.Background())
	defer gs.cancelAllContexts()
	old := gs.phaseOneCtx()
	gs.resetPhaseOneCtx()
	require.ErrorIs(t, old.Err(), context.Canceled)
	require.NoError(t, gs.phaseOneCtx().Err(), "a running runtime may start a new generation")
}

func Test_ShutdownContext_ResetCannotReviveAfterEmergencyCancellation(t *testing.T) {
	gs := NewGracefulShutdown(context.Background())
	gs.cancelAllContexts()
	gs.resetPhaseOneCtx()
	require.ErrorIs(t, gs.phaseOneCtx().Err(), context.Canceled, "promotion after emergency stop must not revive application work")
}

func Test_ShutdownContext_ResetCannotReviveAfterGracefulShutdownBegins(t *testing.T) {
	gs := NewGracefulShutdown(context.Background())
	defer gs.cancelAllContexts()
	gs.beginShutdownPhaseOne()
	gs.resetPhaseOneCtx()
	require.ErrorIs(t, gs.phaseOneCtx().Err(), context.Canceled)
}

func Test_ShutdownContext_ConcurrentResetAndCancellation(t *testing.T) {
	gs := NewGracefulShutdown(context.Background())
	defer gs.cancelAllContexts()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for round := 0; round < 500; round++ {
				switch worker {
				case 0:
					gs.resetPhaseOneCtx()
				case 1:
					gs.cancelPhaseOne()
				case 2:
					gs.cancelAllContexts()
				case 3:
					_ = gs.phaseOneCtx().Err()
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	require.ErrorIs(t, gs.phaseOneCtx().Err(), context.Canceled)
	gs.resetPhaseOneCtx()
	require.ErrorIs(t, gs.phaseOneCtx().Err(), context.Canceled)
}
