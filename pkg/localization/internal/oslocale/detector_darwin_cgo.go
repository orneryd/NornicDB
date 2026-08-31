//go:build darwin && cgo

package oslocale

/*
#cgo LDFLAGS: -framework CoreFoundation
#include <CoreFoundation/CoreFoundation.h>
#include <stdlib.h>

static char* nornic_preferred_language_at(CFArrayRef languages, CFIndex index) {
    CFStringRef value = (CFStringRef)CFArrayGetValueAtIndex(languages, index);
    CFIndex length = CFStringGetMaximumSizeForEncoding(CFStringGetLength(value), kCFStringEncodingUTF8) + 1;
    char* buffer = (char*)malloc((size_t)length);
    if (buffer == NULL) return NULL;
    if (!CFStringGetCString(value, buffer, length, kCFStringEncodingUTF8)) {
        free(buffer);
        return NULL;
    }
    return buffer;
}
*/
import "C"

import (
	"unsafe"
)

func preferenceStrings() ([]string, error) {
	languages := C.CFLocaleCopyPreferredLanguages()
	if unsafe.Pointer(languages) == nil {
		return nil, ErrNotDetected
	}
	defer C.CFRelease(C.CFTypeRef(languages))

	count := int(C.CFArrayGetCount(languages))
	preferences := make([]string, 0, count)
	for index := 0; index < count; index++ {
		value := C.nornic_preferred_language_at(languages, C.CFIndex(index))
		if value == nil {
			continue
		}
		preferences = append(preferences, C.GoString(value))
		C.free(unsafe.Pointer(value))
	}
	if len(preferences) == 0 {
		return nil, ErrNotDetected
	}
	return preferences, nil
}
