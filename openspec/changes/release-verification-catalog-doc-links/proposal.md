# Restore release verification

Both #1487 and #1449 expose failures already present on release/v1.4.4 at
714b95e326cddc6bba449437b1cec542eecb8060: two quantization catalog IDs lack
canonical typed constants, and two performance links leave the MkDocs source
tree. Register the existing message IDs with unchanged behavior and point those
links at the immutable tracked benchmark guide. Verify this common correction
separately, then propagate it to both affected branches before final checks.
