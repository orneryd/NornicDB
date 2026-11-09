package testfixture

import (
	"errors"
	"testing"
)

func TestLifetimeOwnedConstructorResults(t *testing.T) {
	constructorError := errors.New("constructor returned a resource and an error")
	for _, withResource := range []bool{false, true} {
		var resource *lifetimeState
		if withResource {
			resource = &lifetimeState{}
		}
		t.Run("two_results", func(t *testing.T) {
			got, err := OwnResult[*lifetimeState](t, nil)(resource, constructorError)
			if got != resource || err != constructorError {
				t.Fatal("constructor results changed")
			}
		})
		if resource != nil && (!resource.closed || resource.closes != 1) {
			t.Fatal("resource not closed exactly once at its own test boundary")
		}
		if withResource {
			resource = &lifetimeState{}
		}
		metadata := &struct{ recovered int }{7}
		t.Run("three_results", func(t *testing.T) {
			got, result, err := OwnResult3[*lifetimeState, *struct{ recovered int }](t, nil)(resource, metadata, constructorError)
			if got != resource || result != metadata || err != constructorError {
				t.Fatal("recovery results changed")
			}
		})
		if resource != nil && (!resource.closed || resource.closes != 1) {
			t.Fatal("recovered resource not closed exactly once at its test boundary")
		}
	}
}
