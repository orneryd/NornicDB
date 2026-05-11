//go:build cuda

package embed

// localGGUFBackend = "cuda" when the cuda build tag is set
// (amd64-cuda Docker variant). Plan 04-05 D-06a closed enum value.
const localGGUFBackend = "cuda"
