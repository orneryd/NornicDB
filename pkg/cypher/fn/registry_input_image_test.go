package fn

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// valueFunction is a registration's implementation for a test that only reads
// the declared policies.
func valueFunction(_ context.Context, _ Context, _ []string) (interface{}, error) {
	return nil, nil
}

// A registration stores the input-image demand with its implementation: a
// replacement of the name states both policies again, and the demand of the
// replaced implementation is gone with it.
func TestRegisterStoresInputImageWithImplementation(t *testing.T) {
	const name = "input_image_declared"
	Register(name, valueFunction, OpaqueReads, HeaderInputImage)

	image, found := InputImageOf("INPUT_IMAGE_DECLARED")
	require.True(t, found, "the registry owns the registered name")
	require.Equal(t, HeaderInputImage, image, "a lookup is case-insensitive, as Register's name is")

	Register(name, valueFunction, CanonicalReads, FullInputImage)
	image, found = InputImageOf(name)
	require.True(t, found)
	require.Equal(t, FullInputImage, image, "the name carries no promise of its own")

	Register(name, valueFunction, CanonicalReads, StructuralInputImage)
	image, found = InputImageOf(name)
	require.True(t, found)
	require.Equal(t, StructuralInputImage, image)
}

// A name the registry doesn't own reports no registration: a caller decides
// from the owner it has instead of reading a promise nobody made.
func TestInputImageOfReportsNoRegistration(t *testing.T) {
	image, found := InputImageOf("input_image_unregistered")
	require.False(t, found)
	require.Equal(t, FullInputImage, image, "an unregistered name promises nothing")
}

// The zero value is the conservative declaration, so a registration that states
// no demand never narrows the image a custom callback may read.
func TestInputImageZeroValueIsFullScope(t *testing.T) {
	var zero InputImage
	require.Equal(t, FullInputImage, zero)
	require.True(t, zero.Valid())
	require.Equal(t, FullInputImage, InputImage(0))
}

// An unknown demand is rejected where every other registration invariant is:
// Register panics, so a value can't be stored as one promise and read as
// another.
func TestRegisterRejectsUnknownInputImage(t *testing.T) {
	require.PanicsWithValue(t, "fn.Register: unknown input image", func() {
		Register("input_image_unknown", valueFunction, OpaqueReads, InputImage(200))
	})
	_, found := InputImageOf("input_image_unknown")
	require.False(t, found, "a rejected registration stores nothing")
}

// The read observation and the input-image demand are independent policies: a
// canonical read observation never implies a narrow input image, and a full
// input image never implies an observed read.
func TestReadObservationAndInputImageAreIndependent(t *testing.T) {
	Register("input_image_canonical_full", valueFunction, CanonicalReads, FullInputImage)
	image, found := InputImageOf("input_image_canonical_full")
	require.True(t, found)
	require.Equal(t, FullInputImage, image, "complete read observation says nothing about the row bindings")

	Register("input_image_opaque_header", valueFunction, OpaqueReads, HeaderInputImage)
	image, found = InputImageOf("input_image_opaque_header")
	require.True(t, found)
	require.Equal(t, HeaderInputImage, image, "a narrowed image says nothing about the database reads")
}

// The built-in declarations are the reviewed source promises the statement
// planner reads: the entity scalars read a value's header, the value functions
// read their arguments, and the list built-ins keep the planner's structural
// contract.
func TestReviewedBuiltInInputImages(t *testing.T) {
	for name, want := range map[string]InputImage{
		"id":                     HeaderInputImage,
		"elementid":              HeaderInputImage,
		"labels":                 HeaderInputImage,
		"type":                   HeaderInputImage,
		"keys":                   MetadataArgumentInputImage,
		"properties":             MetadataArgumentInputImage,
		"size":                   StructuralInputImage,
		"reveal":                 MetadataArgumentInputImage,
		"graph.names":            MetadataArgumentInputImage,
		"graph.propertiesbyname": MetadataArgumentInputImage,
	} {
		image, found := InputImageOf(name)
		require.True(t, found, name)
		require.Equal(t, want, image, name)
	}
}
