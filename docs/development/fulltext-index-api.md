# Full-text index Go API

`search.FulltextIndex` provides the full-text index's single-document entry
point. Construct it with an analyzer, or pass `nil` for the default analyzer.
Go calls must supply the analysis mode explicitly:

```go
index := search.NewFulltextIndex(nil)
index.Index(id, text, nil, nil, false)
```

With `prepared` set to `false`, the index analyzes the text with its configured
analyzer and ignores the token and frequency arguments. Code that already used
that same analyzer for admission or accounting can reuse its result:

```go
terms := analyzer.Analyze(text)
frequency := make(map[string]int, len(terms))
for _, term := range terms {
    frequency[term]++
}
index.Index(id, text, terms, frequency, true)
```

When `prepared` is `true`, the caller must supply the exact tokens and their
frequencies for the text using the analyzer configured on the index. This avoids
analyzing the same document twice. `IndexBatch` remains the bulk-build API and
analyzes its entries itself. Single and batch writes ignore empty IDs.
