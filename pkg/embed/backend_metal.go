//go:build metal

package embed

// localGGUFBackend = "metal" when the metal build tag is set
// (arm64-metal Docker variant). Plan 04-05 D-06a closed enum value.
const localGGUFBackend = "metal"
