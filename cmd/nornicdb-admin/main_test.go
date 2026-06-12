package main

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/adminimport"
)

func TestExitCodeForError_UsesImportExitCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "nil", err: nil, want: adminimport.ExitOK},
		{name: "plain error", err: errors.New("boom"), want: 1},
		{name: "csv", err: &adminimport.Error{ExitCode: adminimport.ExitCSV, Message: "csv"}, want: adminimport.ExitCSV},
		{name: "wrapped", err: errors.New((&adminimport.Error{ExitCode: adminimport.ExitDuplicateID, Message: "dup"}).Error()), want: 1},
		{name: "wrapped as", err: wrapErr(&adminimport.Error{ExitCode: adminimport.ExitBadRelationship, Message: "bad"}), want: adminimport.ExitBadRelationship},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exitCodeForError(tt.err); got != tt.want {
				t.Fatalf("exitCodeForError() = %d, want %d", got, tt.want)
			}
		})
	}
}

func wrapErr(err error) error {
	return &wrapped{err: err}
}

type wrapped struct {
	err error
}

func (w *wrapped) Error() string { return "wrapped: " + w.err.Error() }
func (w *wrapped) Unwrap() error { return w.err }
