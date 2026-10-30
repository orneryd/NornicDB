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
function framingAcceptedSource(original) {
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
  return framingView(text, edits);
}

// Extent biases match the source owner's start/end mapping. Binary search
// avoids rescanning every prior edit for each statement boundary.
function framingView(text, edits) {
  const originalOffset = (offset, start) => {
    let low = 0, high = edits.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      const acceptedStart = edits[middle][2];
      if (acceptedStart < offset || (start && acceptedStart === offset)) low = middle + 1;
      else high = middle;
    }
    if (low === 0) return offset;
    const [originalStart, originalEnd, , acceptedEnd] = edits[low - 1];
    if (start ? offset < acceptedEnd : offset <= acceptedEnd) return start ? originalStart : originalEnd;
    return offset + originalEnd - acceptedEnd;
  };
  const span = (start, end) => {
    const mappedStart = originalOffset(start, true);
    return [mappedStart, end === start ? mappedStart : originalOffset(end, false)];
  };
  return { text, span };
}

// Expand admitted raw lowercase-u escapes once, before quotes/comments can
// change framing. null means no admitted expansion; false means malformed
// source, which stays opaque for the executor's native diagnostic. Splitting
// must not throw outside the editor's request error handling.
function projectFramingUnicode(original) {
  if (!original.includes('\\u')) return null;
  let text = '', last = 0;
  let edits = null;
  const hex = (at) => {
    if (at + 4 > original.length) return NaN;
    let value = 0;
    for (let j=at;j<at+4;j++) {
      const c=original.charCodeAt(j);
      let digit=c>=48&&c<=57 ? c-48 : c>=65&&c<=70 ? c-55 : c>=97&&c<=102 ? c-87 : -1;
      if (digit<0) return NaN;
      value=(value<<4)|digit;
    }
    return value;
  };
  for (let index=0;index<original.length;) {
    if (original[index] !== '\\' || index+1===original.length) {index++;continue;}
    if (original[index+1] === '\\') {index+=2;continue;}
    if (original[index+1] !== 'u') {index+=2;continue;}
    let value=hex(index+2);
    if (Number.isNaN(value)) return false;
    let end=index+6;
    if (value>=0xd800&&value<=0xdbff&&original.slice(end,end+2)==='\\u') {
      const low=hex(end+2);
      if (low>=0xdc00&&low<=0xdfff) {value=0x10000+(value-0xd800)*0x400+low-0xdc00;end+=6;}
    }
    text+=original.slice(last,index);
    const acceptedStart=text.length;
    text+=String.fromCodePoint(value);
    if (edits === null) edits = [];
    edits.push([index,end,acceptedStart,text.length]);
    last=end;index=end;
  }
  if (edits === null) return null;
  text+=original.slice(last);
  return framingView(text,edits);
}

function framingSource(original) {
  const projected=projectFramingUnicode(original);
  if (projected===false) return null;
  if (projected===null) return framingAcceptedSource(original);
  const accepted=framingAcceptedSource(projected.text);
  return {text:accepted.text,span(start,end) {
    const [a,b]=accepted.span(start,end);
    return projected.span(a,b);
  }};
}

export function cypherStatementSpans(original) {
  const framing = framingSource(original);
  if (framing === null) return [[0, original.length]];
  const { text, span } = framing;
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
