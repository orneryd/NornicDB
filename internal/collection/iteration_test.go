package collection

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestApplyInOrder(t *testing.T) {
	sentinel := errors.New("item rejected")
	for _, input := range []struct {
		name        string
		items, want []int
		failAt      int
	}{
		{name: "nil"}, {name: "empty", items: []int{}},
		{name: "success", items: []int{1, 2, 3}, want: []int{1, 2, 3}},
		{name: "first error", items: []int{1, 2, 3}, want: []int{1, 2}, failAt: 2},
	} {
		t.Run(input.name, func(t *testing.T) {
			var visited []int
			err := ApplyInOrder(input.items, func(item int) error {
				visited = append(visited, item)
				if item == input.failAt {
					return sentinel
				}
				return nil
			})
			if input.failAt != 0 {
				require.ErrorIs(t, err, sentinel)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, input.want, visited)
		})
	}
}
