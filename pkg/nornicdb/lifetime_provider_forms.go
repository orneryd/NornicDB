// Provider capability forms preserve the source method set. Do not advertise a
// document/batch capability merely because the lifetime wrapper can delegate it.
package nornicdb
import ("github.com/orneryd/nornicdb/pkg/embed";"github.com/orneryd/nornicdb/pkg/lifecycle")
func newManagedProvider(source embed.Embedder,resource *lifecycle.Resource) embed.Embedder {
 base:=&managedProvider{source:source,resource:resource};mask:=0
 if _,ok:=source.(embed.TypedEmbedder);ok{mask|=1}
 if _,ok:=source.(embed.DocumentChunkEmbedder);ok{mask|=2}
 if _,ok:=source.(embed.DocumentPropertyChunkEmbedder);ok{mask|=4}
 if _,ok:=source.(embed.DocumentBatchChunkEmbedder);ok{mask|=8}
 if _,ok:=source.(embed.DocumentPropertyBatchChunkEmbedder);ok{mask|=16}
 if _,ok:=source.(embed.EmbeddingSpaceProvider);ok{mask|=32}
 switch mask {
 case 0:return struct{*managedProvider}{base}
 case 1:return struct{*managedProvider;managedTyped}{base,managedTyped{base}}
 case 2:return struct{*managedProvider;managedDocumentChunks}{base,managedDocumentChunks{base}}
 case 3:return struct{*managedProvider;managedTyped;managedDocumentChunks}{base,managedTyped{base},managedDocumentChunks{base}}
 case 4:return struct{*managedProvider;managedPropertyChunks}{base,managedPropertyChunks{base}}
 case 5:return struct{*managedProvider;managedTyped;managedPropertyChunks}{base,managedTyped{base},managedPropertyChunks{base}}
 case 6:return struct{*managedProvider;managedDocumentChunks;managedPropertyChunks}{base,managedDocumentChunks{base},managedPropertyChunks{base}}
 case 7:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyChunks}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyChunks{base}}
 case 8:return struct{*managedProvider;managedDocumentBatch}{base,managedDocumentBatch{base}}
 case 9:return struct{*managedProvider;managedTyped;managedDocumentBatch}{base,managedTyped{base},managedDocumentBatch{base}}
 case 10:return struct{*managedProvider;managedDocumentChunks;managedDocumentBatch}{base,managedDocumentChunks{base},managedDocumentBatch{base}}
 case 11:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedDocumentBatch}{base,managedTyped{base},managedDocumentChunks{base},managedDocumentBatch{base}}
 case 12:return struct{*managedProvider;managedPropertyChunks;managedDocumentBatch}{base,managedPropertyChunks{base},managedDocumentBatch{base}}
 case 13:return struct{*managedProvider;managedTyped;managedPropertyChunks;managedDocumentBatch}{base,managedTyped{base},managedPropertyChunks{base},managedDocumentBatch{base}}
 case 14:return struct{*managedProvider;managedDocumentChunks;managedPropertyChunks;managedDocumentBatch}{base,managedDocumentChunks{base},managedPropertyChunks{base},managedDocumentBatch{base}}
 case 15:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyChunks;managedDocumentBatch}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyChunks{base},managedDocumentBatch{base}}
 case 16:return struct{*managedProvider;managedPropertyBatch}{base,managedPropertyBatch{base}}
 case 17:return struct{*managedProvider;managedTyped;managedPropertyBatch}{base,managedTyped{base},managedPropertyBatch{base}}
 case 18:return struct{*managedProvider;managedDocumentChunks;managedPropertyBatch}{base,managedDocumentChunks{base},managedPropertyBatch{base}}
 case 19:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyBatch}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyBatch{base}}
 case 20:return struct{*managedProvider;managedPropertyChunks;managedPropertyBatch}{base,managedPropertyChunks{base},managedPropertyBatch{base}}
 case 21:return struct{*managedProvider;managedTyped;managedPropertyChunks;managedPropertyBatch}{base,managedTyped{base},managedPropertyChunks{base},managedPropertyBatch{base}}
 case 22:return struct{*managedProvider;managedDocumentChunks;managedPropertyChunks;managedPropertyBatch}{base,managedDocumentChunks{base},managedPropertyChunks{base},managedPropertyBatch{base}}
 case 23:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyChunks;managedPropertyBatch}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyChunks{base},managedPropertyBatch{base}}
 case 24:return struct{*managedProvider;managedDocumentBatch;managedPropertyBatch}{base,managedDocumentBatch{base},managedPropertyBatch{base}}
 case 25:return struct{*managedProvider;managedTyped;managedDocumentBatch;managedPropertyBatch}{base,managedTyped{base},managedDocumentBatch{base},managedPropertyBatch{base}}
 case 26:return struct{*managedProvider;managedDocumentChunks;managedDocumentBatch;managedPropertyBatch}{base,managedDocumentChunks{base},managedDocumentBatch{base},managedPropertyBatch{base}}
 case 27:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedDocumentBatch;managedPropertyBatch}{base,managedTyped{base},managedDocumentChunks{base},managedDocumentBatch{base},managedPropertyBatch{base}}
 case 28:return struct{*managedProvider;managedPropertyChunks;managedDocumentBatch;managedPropertyBatch}{base,managedPropertyChunks{base},managedDocumentBatch{base},managedPropertyBatch{base}}
 case 29:return struct{*managedProvider;managedTyped;managedPropertyChunks;managedDocumentBatch;managedPropertyBatch}{base,managedTyped{base},managedPropertyChunks{base},managedDocumentBatch{base},managedPropertyBatch{base}}
 case 30:return struct{*managedProvider;managedDocumentChunks;managedPropertyChunks;managedDocumentBatch;managedPropertyBatch}{base,managedDocumentChunks{base},managedPropertyChunks{base},managedDocumentBatch{base},managedPropertyBatch{base}}
 case 31:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyChunks;managedDocumentBatch;managedPropertyBatch}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyChunks{base},managedDocumentBatch{base},managedPropertyBatch{base}}
 case 32:return struct{*managedProvider;managedEmbeddingSpace}{base,managedEmbeddingSpace{base}}
 case 33:return struct{*managedProvider;managedTyped;managedEmbeddingSpace}{base,managedTyped{base},managedEmbeddingSpace{base}}
 case 34:return struct{*managedProvider;managedDocumentChunks;managedEmbeddingSpace}{base,managedDocumentChunks{base},managedEmbeddingSpace{base}}
 case 35:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentChunks{base},managedEmbeddingSpace{base}}
 case 36:return struct{*managedProvider;managedPropertyChunks;managedEmbeddingSpace}{base,managedPropertyChunks{base},managedEmbeddingSpace{base}}
 case 37:return struct{*managedProvider;managedTyped;managedPropertyChunks;managedEmbeddingSpace}{base,managedTyped{base},managedPropertyChunks{base},managedEmbeddingSpace{base}}
 case 38:return struct{*managedProvider;managedDocumentChunks;managedPropertyChunks;managedEmbeddingSpace}{base,managedDocumentChunks{base},managedPropertyChunks{base},managedEmbeddingSpace{base}}
 case 39:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyChunks;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyChunks{base},managedEmbeddingSpace{base}}
 case 40:return struct{*managedProvider;managedDocumentBatch;managedEmbeddingSpace}{base,managedDocumentBatch{base},managedEmbeddingSpace{base}}
 case 41:return struct{*managedProvider;managedTyped;managedDocumentBatch;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentBatch{base},managedEmbeddingSpace{base}}
 case 42:return struct{*managedProvider;managedDocumentChunks;managedDocumentBatch;managedEmbeddingSpace}{base,managedDocumentChunks{base},managedDocumentBatch{base},managedEmbeddingSpace{base}}
 case 43:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedDocumentBatch;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentChunks{base},managedDocumentBatch{base},managedEmbeddingSpace{base}}
 case 44:return struct{*managedProvider;managedPropertyChunks;managedDocumentBatch;managedEmbeddingSpace}{base,managedPropertyChunks{base},managedDocumentBatch{base},managedEmbeddingSpace{base}}
 case 45:return struct{*managedProvider;managedTyped;managedPropertyChunks;managedDocumentBatch;managedEmbeddingSpace}{base,managedTyped{base},managedPropertyChunks{base},managedDocumentBatch{base},managedEmbeddingSpace{base}}
 case 46:return struct{*managedProvider;managedDocumentChunks;managedPropertyChunks;managedDocumentBatch;managedEmbeddingSpace}{base,managedDocumentChunks{base},managedPropertyChunks{base},managedDocumentBatch{base},managedEmbeddingSpace{base}}
 case 47:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyChunks;managedDocumentBatch;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyChunks{base},managedDocumentBatch{base},managedEmbeddingSpace{base}}
 case 48:return struct{*managedProvider;managedPropertyBatch;managedEmbeddingSpace}{base,managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 49:return struct{*managedProvider;managedTyped;managedPropertyBatch;managedEmbeddingSpace}{base,managedTyped{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 50:return struct{*managedProvider;managedDocumentChunks;managedPropertyBatch;managedEmbeddingSpace}{base,managedDocumentChunks{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 51:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyBatch;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 52:return struct{*managedProvider;managedPropertyChunks;managedPropertyBatch;managedEmbeddingSpace}{base,managedPropertyChunks{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 53:return struct{*managedProvider;managedTyped;managedPropertyChunks;managedPropertyBatch;managedEmbeddingSpace}{base,managedTyped{base},managedPropertyChunks{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 54:return struct{*managedProvider;managedDocumentChunks;managedPropertyChunks;managedPropertyBatch;managedEmbeddingSpace}{base,managedDocumentChunks{base},managedPropertyChunks{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 55:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyChunks;managedPropertyBatch;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyChunks{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 56:return struct{*managedProvider;managedDocumentBatch;managedPropertyBatch;managedEmbeddingSpace}{base,managedDocumentBatch{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 57:return struct{*managedProvider;managedTyped;managedDocumentBatch;managedPropertyBatch;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentBatch{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 58:return struct{*managedProvider;managedDocumentChunks;managedDocumentBatch;managedPropertyBatch;managedEmbeddingSpace}{base,managedDocumentChunks{base},managedDocumentBatch{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 59:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedDocumentBatch;managedPropertyBatch;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentChunks{base},managedDocumentBatch{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 60:return struct{*managedProvider;managedPropertyChunks;managedDocumentBatch;managedPropertyBatch;managedEmbeddingSpace}{base,managedPropertyChunks{base},managedDocumentBatch{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 61:return struct{*managedProvider;managedTyped;managedPropertyChunks;managedDocumentBatch;managedPropertyBatch;managedEmbeddingSpace}{base,managedTyped{base},managedPropertyChunks{base},managedDocumentBatch{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 62:return struct{*managedProvider;managedDocumentChunks;managedPropertyChunks;managedDocumentBatch;managedPropertyBatch;managedEmbeddingSpace}{base,managedDocumentChunks{base},managedPropertyChunks{base},managedDocumentBatch{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 case 63:return struct{*managedProvider;managedTyped;managedDocumentChunks;managedPropertyChunks;managedDocumentBatch;managedPropertyBatch;managedEmbeddingSpace}{base,managedTyped{base},managedDocumentChunks{base},managedPropertyChunks{base},managedDocumentBatch{base},managedPropertyBatch{base},managedEmbeddingSpace{base}}
 };panic("unreachable provider capability mask")
}
