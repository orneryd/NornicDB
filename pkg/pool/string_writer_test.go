// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Exercise the existing builder's byte-writer protocol and retained-result ownership.
package pool

import (
	"fmt"
	"io"
	"testing"
)

var _ io.Writer = (*PooledStringBuilder)(nil)

func TestPooledStringBuilderWriterOwnsBytes(t *testing.T) {
	builder := GetStringBuilder()
	defer PutStringBuilder(builder)
	input := []byte("prefix")
	count, err := builder.Write(input)
	if err != nil || count != len(input) {
		t.Fatalf("Write = %d, %v", count, err)
	}
	input[0] = 'X'
	if _, err := fmt.Fprint(builder, "/tail"); err != nil {
		t.Fatal(err)
	}
	retained := builder.String()
	builder.Reset()
	builder.WriteString("replacement")
	if retained != "prefix/tail" || builder.String() != "replacement" {
		t.Fatalf("writer or retained-string ownership: %q, %q", retained, builder.String())
	}
}
