# Storage serialization

NornicDB selects its storage encoding internally. The node and relationship codecs record the format needed to read each stored record. The former `storage_serializer` YAML setting and `NORNICDB_STORAGE_SERIALIZER` environment variable have been removed.

For supported upgrade and recovery procedures, see [Recovery format compatibility](recovery-format-compatibility.md).
