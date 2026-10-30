//go:build vulkan

package embed

// localGGUFBackend = "vulkan" when the vulkan build tag is set
// (amd64-vulkan Docker variant). Plan 04-05 D-06a closed enum value.
const localGGUFBackend = "vulkan"
