// Browser peer of internal/cyphertext's opaque-token and statement extents.
// The shared statement_spans.json fixtures exercise this language boundary.
export function skipCypherToken(text, start) {
  const quote = text[start];
  if (quote === "'" || quote === '"' || quote === '`') {
    for (let index = start + 1; index < text.length; index++) {
      if (text[index] === '\\' && quote !== '`' && index + 1 < text.length) {
        index++;
        continue;
      }
      if (text[index] !== quote) continue;
      if (quote === '`' && text[index + 1] === '`') {
        index++;
        continue;
      }
      return index + 1;
    }
    return text.length;
  }
  if (text.startsWith('//', start)) {
    let end = start + 2;
    while (end < text.length && text[end] !== '\n' && text[end] !== '\r') end++;
    return end;
  }
  if (text.startsWith('/*', start)) {
    const end = text.indexOf('*/', start + 2);
    return end < 0 ? text.length : end + 2;
  }
  return start;
}

// Only deletion, whitespace and the fullwidth semicolon can affect framing.
// Other accepted punctuation/arrow replacements cannot introduce a quote,
// comment or delimiter. Execution retains the full normalization authority.
function framingSource(original) {
  let text = '';
  const edits = [];
  const deleted = new Set(['\u200b', '\u200c', '\u200d', '\u2060', '\ufeff']);
  const spaces = new Set(['\v', '\f', '\u00a0', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200a', '\u202f', '\u205f', '\u3000']);
  const lines = new Set(['\u0085', '\u2028', '\u2029']);
  for (let index = 0; index < original.length;) {
    const end = skipCypherToken(original, index);
    if (end > index) {
      text += original.slice(index, end);
      index = end;
      continue;
    }
    const character = original[index];
    const replacement = deleted.has(character) ? '' : character === '；' ? ';' : lines.has(character) ? '\n' : spaces.has(character) ? ' ' : character;
    if (character !== replacement) edits.push([index, index + 1, text.length, text.length + replacement.length]);
    text += replacement;
    index++;
  }
  const originalOffset = (offset, start) => {
    let shift = 0;
    for (const [originalStart, originalEnd, acceptedStart, acceptedEnd] of edits) {
      if (start) {
        if (offset < acceptedStart) break;
        if (offset < acceptedEnd) return originalStart;
      } else {
        if (offset <= acceptedStart) break;
        if (offset <= acceptedEnd) return originalEnd;
      }
      shift = originalEnd - acceptedEnd;
    }
    return offset + shift;
  };
  const span = (start, end) => {
    const mappedStart = originalOffset(start, true);
    return [mappedStart, end === start ? mappedStart : originalOffset(end, false)];
  };
  return { text, span };
}

export function cypherStatementSpans(original) {
  const { text, span } = framingSource(original);
  const spans = [];
  let start = 0;
  for (let index = 0; index < text.length; index++) {
    const end = skipCypherToken(text, index);
    if (end > index) {
      index = end - 1;
      continue;
    }
    if (text[index] === ';') {
      spans.push(span(start, index));
      start = index + 1;
    }
  }
  spans.push(span(start, text.length));
  return spans;
}
