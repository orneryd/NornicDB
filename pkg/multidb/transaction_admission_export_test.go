package multidb

import (
	"context"
	"fmt"
)

// AcquireWriteAdmissionLeaseForTest exposes only the existing admission owner
// to external-package Bolt tests, which cannot import Bolt from package multidb
// without an import cycle. It is absent from production builds.
func (m *DatabaseManager) AcquireWriteAdmissionLeaseForTest(ctx context.Context, databaseName string, exclusive bool) (func(), error) {
	engine, err := m.GetStorage(databaseName)
	if err != nil {
		return nil, err
	}
	tracker, ok := engine.(*sizeTrackingEngine)
	if !ok || tracker.writeMu == nil {
		return nil, fmt.Errorf("database %q has no managed write admission owner", databaseName)
	}
	return tracker.writeMu.acquire(ctx, exclusive, false)
}
