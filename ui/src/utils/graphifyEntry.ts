export interface EntryCandidate {
 id:string; label:string; source_file?:string; node_kind?:string; symbol_kind?:string;
 degree?:number; explicit?:boolean; internalId?:string;
}
export interface RankedEntry extends EntryCandidate {score:number;reason:string}

/** Paths are normalized once for both entry ranking and the visible test filter. */
export function isTestSource(sourceFile:string|undefined):boolean {
 const path=(sourceFile??'').replace(/\\/g,'/').toLowerCase(),name=path.split('/').pop()??'';
 return /(^|\/)(__tests__|tests?|testing|specs?)(\/|$)/.test(path)||
  /(_test\.go|\.test\.[^.]+|\.spec\.[^.]+|_spec\.rb)$/.test(name)||
  /^test_/.test(name)||/Test\.(java|kt|cs)$/i.test(name);
}

/** Suggestions, not a claim that arbitrary repositories have a single executable entry. */
export function rankEntryCandidates(candidates:EntryCandidate[]):RankedEntry[] {
 const unique=new Map<string,RankedEntry>();
 for(const node of candidates){
  if(!node.id||isTestSource(node.source_file))continue;
  const path=(node.source_file??'').replace(/\\/g,'/').toLowerCase();
  if(/(^|\/)(node_modules|vendor|dist|build|generated|\.venv)(\/|$)/.test(path))continue;
  const name=(node.label||node.id).replace(/\([^)]*\)\s*$/,'').split(/[.:/#]/).pop()?.toLowerCase()??'';
  let score=0,reason='Connected source symbol';
  if(node.explicit){score=1000;reason='Marked Main in the graph';}
  else if(['main','__main__','wmain','winmain'].includes(name)){score=700;reason='Conventional executable entry';}
  else if(/(^|\/)(__main__\.py|program\.cs|main\.[^.]+)$/.test(path)){score=500;reason='Executable entry file';}
  else if(/(^|\/)(app|server|index)\.(js|ts|jsx|tsx|mjs|cjs|py|rb)$/.test(path)&&['bootstrap','start','run','app','handler','createapp','createserver','default'].includes(name)){score=400;reason='Application bootstrap';}
  else if(['bootstrap','startapplication','application','createapp'].includes(name)){score=300;reason='Application entry convention';}
  else if(['handle','handler','lambda_handler'].includes(name)){score=200;reason='Request or serverless entry';}
  score+=Math.min(99,Math.log2(Math.max(0,Number(node.degree)||0)+1)*8);
  const ranked={...node,score,reason};
  if(!unique.has(node.id)||unique.get(node.id)!.score<score)unique.set(node.id,ranked);
 }
 return [...unique.values()].sort((a,b)=>b.score-a.score||a.id.localeCompare(b.id));
}

// Named candidates are fetched independently of connectivity, so a small main()
// is not crowded out by highly connected utilities in large repositories.
const candidateFields='a.id AS id, a.label AS label, a.source_file AS source_file, a.node_kind AS node_kind, a.symbol_kind AS symbol_kind';
const callable="(a.symbol_kind IN ['function','method','constructor'] OR a.node_kind IN ['function','method','function_definition','function_declaration','method_definition','method_declaration'])";
export const MAIN_NODE_QUERY=`MATCH (a:Main) RETURN ${candidateFields} ORDER BY a.id LIMIT 128`;
export const NAMED_ENTRY_QUERY=`MATCH (a) WHERE a.id IS NOT NULL AND a.source_file <> '' AND ${callable}
AND (toLower(a.label) =~ $entryPattern OR toLower(a.source_file) =~ $entryFilePattern)
RETURN ${candidateFields} ORDER BY a.id LIMIT 128`;
// Connectivity is sampled through the existing bounded neighborhood API instead
// of an unbounded undirected Cypher expansion over the entire code graph.
export const ENTRY_CANDIDATE_QUERY=`MATCH (a)
WHERE ${callable} AND a.source_file <> '' AND a.id IS NOT NULL
RETURN ${candidateFields}, id(a) AS internalId ORDER BY a.id LIMIT 128`;

export const NAMED_ENTRY_PARAMS={
 entryPattern:String.raw`.*(^|[.:/#])(main|__main__|wmain|winmain|bootstrap|start|run|handler|lambda_handler|createapp|createserver)(\(.*\))?$`,
 entryFilePattern:String.raw`(^|.*[\\/])(main\.[^/]+|__main__\.py|program\.cs)$`,
};
