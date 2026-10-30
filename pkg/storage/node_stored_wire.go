package storage

import "github.com/vmihailenco/msgpack/v5"

// EncodeMsgpack preserves the stored node map independently of its in-memory
// runtime representation. Keys, value types and field order match the existing
// reflected Node codec; query provenance and embedding identity are not stored.
func (node Node) EncodeMsgpack(enc *msgpack.Encoder) error {
	metadata := node.RuntimeMetadata()
	if err := enc.EncodeMapLen(10); err != nil {
		return err
	}
	if err := enc.EncodeString("ID"); err != nil {
		return err
	}
	if err := enc.EncodeString(string(node.ID)); err != nil {
		return err
	}
	if err := enc.EncodeString("Labels"); err != nil {
		return err
	}
	if err := enc.Encode(node.Labels); err != nil {
		return err
	}
	if err := enc.EncodeString("Properties"); err != nil {
		return err
	}
	if err := enc.Encode(node.Properties); err != nil {
		return err
	}
	if err := enc.EncodeString("CreatedAt"); err != nil {
		return err
	}
	if err := enc.EncodeTime(node.CreatedAt); err != nil {
		return err
	}
	if err := enc.EncodeString("UpdatedAt"); err != nil {
		return err
	}
	if err := enc.EncodeTime(node.UpdatedAt); err != nil {
		return err
	}
	if err := enc.EncodeString("NamedEmbeddings"); err != nil {
		return err
	}
	if err := enc.Encode(node.NamedEmbeddings); err != nil {
		return err
	}
	if err := enc.EncodeString("ChunkEmbeddings"); err != nil {
		return err
	}
	if err := enc.Encode(node.ChunkEmbeddings); err != nil {
		return err
	}
	if err := enc.EncodeString("EmbedMeta"); err != nil {
		return err
	}
	if err := enc.Encode(node.EmbedMeta); err != nil {
		return err
	}
	if err := enc.EncodeString("EmbeddingsStoredSeparately"); err != nil {
		return err
	}
	if err := enc.EncodeBool(metadata.EmbeddingsStoredSeparately); err != nil {
		return err
	}
	if err := enc.EncodeString("VisibilitySuppressed"); err != nil {
		return err
	}
	if err := enc.EncodeBool(metadata.VisibilitySuppressed); err != nil {
		return err
	}
	return nil
}

// DecodeMsgpack reads the unchanged stored node map, including historical maps
// with absent flags or extra fields. Existing runtime identity is not decoded.
func (node *Node) DecodeMsgpack(dec *msgpack.Decoder) error {
	count, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	if count == -1 {
		*node = Node{}
		return nil
	}
	metadata := node.RuntimeMetadata()
	defer func() { node.SetRuntimeMetadata(metadata) }()
	for i := 0; i < count; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		switch key {
		case "ID":
			err = dec.Decode(&node.ID)
		case "Labels":
			err = dec.Decode(&node.Labels)
		case "Properties":
			err = dec.Decode(&node.Properties)
		case "CreatedAt":
			err = dec.Decode(&node.CreatedAt)
		case "UpdatedAt":
			err = dec.Decode(&node.UpdatedAt)
		case "NamedEmbeddings":
			err = dec.Decode(&node.NamedEmbeddings)
		case "ChunkEmbeddings":
			err = dec.Decode(&node.ChunkEmbeddings)
		case "EmbedMeta":
			err = dec.Decode(&node.EmbedMeta)
		case "EmbeddingsStoredSeparately":
			err = dec.Decode(&metadata.EmbeddingsStoredSeparately)
		case "VisibilitySuppressed":
			err = dec.Decode(&metadata.VisibilitySuppressed)
		default:
			err = dec.Skip()
		}
		if err != nil {
			return err
		}
	}
	return nil
}
