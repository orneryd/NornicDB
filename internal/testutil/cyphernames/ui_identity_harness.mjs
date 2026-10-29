// Test-only host for whole production TS/TSX modules. It records real public
// component outputs and registered editor callbacks; it has no name decoder.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
const require = createRequire(import.meta.url);
const ts = require(process.argv[2]);
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const names = await import(path.join(root, 'ui/src/utils/cypherNames.mjs'));
const input = process.argv[3] ? JSON.parse(fs.readFileSync(process.argv[3], 'utf8')) : {};
const driverPackage = process.argv[4] ? require(path.join(process.argv[4], 'node_modules/neo4j-driver')) : null;
const gridCore = require(process.argv[5]);
let states = [], stateIndex = 0, effects = [];
const jsx = (type, props) => ({type, props});
const React = {
  createContext: value => ({value, Provider: 'Provider'}), useContext: context => context.value,
  useId: () => 'identity-test', useMemo: fn => fn(), useCallback: fn => fn,
  useRef: value => ({current:value}), useEffect: fn => {effects.push(fn);},
  useState: value => {const i=stateIndex++; if (!(i in states)) states[i]=typeof value==='function'?value():value;
    return [states[i], next => {states[i]=typeof next==='function'?next(states[i]):next;}];},
};
const UiGrid = function UiGrid() {};
const cache = new Map();
function load(relative) {
  let file = path.resolve(root, relative);
  if (!fs.existsSync(file)) file = ['.ts','.tsx','.mjs'].map(ext=>file+ext).find(fs.existsSync);
  assert.ok(file, `missing production module ${relative}`);
  if (cache.has(file)) return cache.get(file).exports;
  if (file.endsWith('cypherNames.mjs')) return names;
  const module = {exports:{}}; cache.set(file,module);
  const source = fs.readFileSync(file,'utf8').replaceAll('import.meta.env', '({})');
  let compiled = ts.transpileModule(source,{fileName:file,compilerOptions:{target:ts.ScriptTarget.ES2022,
    module:ts.ModuleKind.CommonJS,jsx:ts.JsxEmit.ReactJSX,esModuleInterop:true}}).outputText;
  if (/ui[/\\]src[/\\]pages[/\\](Cyber|Demo|Graphify)\.tsx$/.test(file)) {
    compiled += '\nmodule.exports.testRowProjection = rowsFromCypher;';
  }
  function imports(name) {
    if (name==='react') return React;
    if (name==='react/jsx-runtime') return {jsx,jsxs:jsx,Fragment:'Fragment'};
    if (name==='@ornery/ui-grid-react') return {UiGrid};
    if (name==='@ornery/ui-grid-core') return {buildGridPipeline(){},registerRustWasmGridEngine(){}};
    if (name==='neo4j-driver') return driverPackage ?? {types:{Integer:class{},Node:class{},Relationship:class{},UnboundRelationship:class{}},isInt:()=>false};
    if (name==='react-router-dom') return {useNavigate:()=>()=>{}};
    if (/common\/(PageLayout|PageHeader|Button|Alert|FormInput|Modal)$/.test(name)) {
      const component=name.split('/').at(-1);return {[component]:component};
    }
    if (name==='three') return new Proxy({}, {get:()=>class {}});
    if (name==='3d-force-graph') return {default(){}};
    if (name==='d3-force-3d') return {};
    if (name==='lucide-react') return new Proxy({}, {get:(_target,key)=>String(key)});
    if (name.endsWith('ExpandableCell') || name.endsWith('JsonPreview')) return {};
    if (name.startsWith('.')) return load(path.resolve(path.dirname(file),name));
    throw Error(`unmodeled UI dependency ${name}`);
  }
  const execute = vm.runInThisContext(`(function(require,module,exports){${compiled}\n})`,{filename:file});
  execute(imports,module,module.exports);
  return module.exports;
}
function render(component, props, initial=[]) {
  states=initial; stateIndex=0; effects=[];
  const tree=component(props);
  for (const effect of effects) effect();
  return tree;
}
function elements(tree, predicate, out=[]) {
  if (Array.isArray(tree)) {for(const child of tree) elements(child,predicate,out);return out;}
  if (!tree || typeof tree!=='object') return out;
  if (predicate(tree)) out.push(tree);
  if (tree.props) elements(tree.props.children,predicate,out);
  return out;
}
const failures=[], commands=[], observations=[];
async function check(name, fn) {
  try {await fn();observations.push(name);console.error('UI_IDENTITY_PASS',name);}
  catch(error){failures.push({name,message:error.stack});console.error('UI_IDENTITY_FAIL',name,error.message);}
}
const api=load('ui/src/utils/api.ts').api;
const editing=load('ui/src/utils/knowledgePolicyEditing.ts');
const nodeUtils=load('ui/src/utils/nodeUtils.ts');

if (input.boltUri) {
  const originalWireRun=api.runCypherOverBolt.bind(api);
  api.runCypherOverBolt=async(...args)=>{try{return await originalWireRun(...args);}catch(error){console.error('OWNED_BOLT_CONVERSION_ERROR',error.stack);throw error;}};
  await check('real_Bolt_driver_input_to_UI',async()=>{
    assert.ok(driverPackage,'locked production driver required');
    const driver=driverPackage.driver(input.boltUri,{scheme:'none'},{disableLosslessIntegers:true,connectionTimeout:5000,connectionAcquisitionTimeout:5000,maxConnectionPoolSize:1});
    const session=driver.session({database:'nornic'});
    const query='RETURN {` `:7, nested:{`  `:8, `\t`:9, `a.b`:10, `a``b`:11, `constructor`:12}} AS value';
    try {
      const result=await session.run(query);
      const value=result.records[0].get('value');
      console.error('BOLT_INPUT_SHAPE',JSON.stringify({driver:'6.2.0',ownKeys:Object.keys(value),hasDecodedKey:Object.hasOwn(value,' '),value}));
      assert.ok(Object.hasOwn(value,' '),'runtime Bolt map has own decoded key before UI conversion');
      assert.equal(value[' '],7);
      for (const key of ['  ','\t','a.b','a`b','constructor']) assert.ok(Object.hasOwn(value.nested,key),`runtime nested key ${JSON.stringify(key)}`);
      console.error('BOLT_INPUT_SHAPE',JSON.stringify({driver:'6.2.0',ownKeys:Object.keys(value),value}));
      await session.close();
      api.fetchDiscovery=async()=>null;
      api.getBoltDriver=async()=>driver;
      const converted=await api.executeCypherOnDatabase('nornic',query);
      assert.deepEqual(JSON.parse(JSON.stringify(converted.results[0].data[0].row[0])),value);
    } finally {await session.close();await driver.close();}
  });
  for (const kind of ['map','node','relationship','integer_map','integer','large_integer']) await check(kind==='map'?'real_Bolt_entity_field_names_are_data':`real_Bolt_native_${kind}`,async()=>{
    const driver=driverPackage.driver(input.boltUri,{scheme:'none'},{disableLosslessIntegers:!(kind==='integer'||kind==='large_integer'),connectionTimeout:5000,connectionAcquisitionTimeout:5000,maxConnectionPoolSize:1});
    const session=driver.session({database:'nornic'});
    try {
      if(kind==='node') await session.run('CREATE (:R5WireNative {value:7})');
      if(kind==='relationship') await session.run('CREATE (:R5WireLeft)-[:R5WireRel {value:7}]->(:R5WireRight)');
      const query=kind==='integer'?'RETURN 7 AS value':kind==='large_integer'?'RETURN 9007199254740993 AS value':kind==='integer_map'?"RETURN {low:7, high:0, `__isInteger__`:true, note:'retained'} AS value":kind==='node'?'MATCH (n:R5WireNative) RETURN n AS value':kind==='relationship'?'MATCH ()-[r:R5WireRel]->() RETURN r AS value':"RETURN {identity:7, properties:{value:8}, note:'retained', `__isNode__`:true, `__isRelationship__`:true} AS value";
      const result=await session.run(query);
      const value=result.records[0].get('value');
      assert.equal(value instanceof driverPackage.types.Node,kind==='node','actual driver class distinguishes a graph node from a data map');
      assert.equal(value instanceof driverPackage.types.Relationship,kind==='relationship');
      assert.equal(value instanceof driverPackage.types.Integer,kind==='integer'||kind==='large_integer');
      if(kind==='integer_map') assert.deepEqual(Object.keys(value).sort(),['__isInteger__','high','low','note']);
      console.error('BOLT_INPUT_KIND',JSON.stringify({kind,ownKeys:Object.keys(value),isDriverNode:value instanceof driverPackage.types.Node,isDriverRelationship:value instanceof driverPackage.types.Relationship,isDriverInteger:value instanceof driverPackage.types.Integer,vendorIsInt:driverPackage.isInt(value)}));
      await session.close();
      api.fetchDiscovery=async()=>null;api.getBoltDriver=async()=>driver;api.boltUnavailable=false;
      const converted=(await api.executeCypherOnDatabase('nornic',query)).results[0].data[0].row[0];
      if(kind==='integer'||kind==='large_integer') {
        assert.equal(converted,kind==='integer'?7:'9007199254740993','native integers retain numeric/string conversion');
      } else if(kind==='node') {
        assert.equal(converted.elementId,value.elementId);assert.equal(converted.properties.value,7);
        assert.ok(converted.labels.includes('R5WireNative'));
      } else if(kind==='relationship') {
        assert.equal(converted.type,'R5WireRel');assert.equal(converted.properties.value,7);
        assert.equal(converted.elementId,value.elementId);
        assert.equal(converted.startNodeElementId,value.startNodeElementId);
        assert.equal(converted.endNodeElementId,value.endNodeElementId);
      } else {
        assert.deepEqual(JSON.parse(JSON.stringify(converted)),JSON.parse(JSON.stringify(value)),'map keys that resemble entity fields remain data');
      }
    } finally {await session.close();await driver.close();}
  });
  console.log(JSON.stringify({commands,failures,observations}));
  process.exit(failures.length ? 1 : 0);
}

const resultCases=input.results ?? [
    {columns:['__proto__'],data:[{row:[7],meta:[]}]},
    {columns:['__gridId'],data:[{row:[0],meta:[]},{row:[0],meta:[]}]},
    {columns:['nornicSelect','__nodeId','__nodeData','constructor',' ','\t','a.b'],data:[{row:[1,2,3,4,5,6,7],meta:[]}]},
    {columns:['a.b','a','constructor','prototype','plain'],data:[{row:[11,{b:99},12,13,14],meta:[]}]},
  ];
for (const [caseIndex,result] of resultCases.entries()) await check(`result_grid_public_columns/${caseIndex}`,()=>{
    const {QueryResultsTable}=load('ui/src/components/browser/QueryResultsTable.tsx');
    const tree=render(QueryResultsTable,{cypherResult:{results:[result]},selectedNodeIds:new Set(),onNodeSelect(){},onSelectionChange(){}});
    const grid=elements(tree,e=>e.type===UiGrid)[0]; assert.ok(grid);
    const options=grid.props.options;
    assert.equal(new Set(options.columnDefs.map(c=>c.name)).size,options.columnDefs.length,'internal and result column IDs must be independent');
    assert.equal(new Set(options.data.map(options.rowIdentity)).size,result.data.length,'row identity is independent of returned data');
    result.data.forEach((row,i)=>result.columns.forEach((column,j)=>{
      assert.ok(Object.hasOwn(options.data[i],column),`missing own result column ${column}`);
      assert.deepEqual(options.data[i][column],row.row[j]);
      const definition=options.columnDefs.find(candidate=>candidate.name===column);
      assert.deepEqual(gridCore.getCellValue(options.data[i],definition),row.row[j],`actual grid accessor for ${JSON.stringify(column)}`);
      const context=gridCore.buildGridCellContext({entity:options.data[i],index:i},definition);
      assert.deepEqual(context.value,row.row[j],`actual grid render context for ${JSON.stringify(column)}`);
      if(row.row[j]!==null && typeof row.row[j]!=='object') {
        const cell=grid.props.cellRenderers[column](context);
        assert.equal(cell.props.children,String(row.row[j]),`actual result renderer for ${JSON.stringify(column)}`);
      }
      assert.equal(options.isRowSelectable({entity:options.data[i]}),false,'scalar aliases do not become node metadata');
    }));
});
await check('result_grid_scalar_renderer_namespace',()=>{
  const {QueryResultsTable}=load('ui/src/components/browser/QueryResultsTable.tsx');
  const result=resultCases[2];
  const tree=render(QueryResultsTable,{cypherResult:{results:[result]},selectedNodeIds:new Set(),onNodeSelect(){},onSelectionChange(){}});
  const grid=elements(tree,e=>e.type===UiGrid)[0];
  const cell=grid.props.cellRenderers.nornicSelect({row:grid.props.options.data[0],value:1,column:{name:'nornicSelect'}});
  assert.equal(cell.props.children,'1','the returned data column uses the data renderer');
});
if(input.nodeResult) await check('result_grid_node_control_namespace',()=>{
  const {QueryResultsTable}=load('ui/src/components/browser/QueryResultsTable.tsx');
  let selected;
  const tree=render(QueryResultsTable,{cypherResult:{results:[input.nodeResult]},selectedNodeIds:new Set(),onNodeSelect:node=>{selected=node;},onSelectionChange(){}});
  const grid=elements(tree,e=>e.type===UiGrid)[0];
  const options=grid.props.options,row=options.data[0];
  assert.equal(options.isRowSelectable({entity:row}),true,'internal node identity is separate from returned scalar aliases');
  const control=options.columnDefs[0].name;
  const cell=grid.props.cellRenderers[control]({row,column:{name:control}});
  const button=elements(cell,e=>e.type==='button')[0];assert.ok(button,'actual Select control');button.props.onClick();
  assert.equal(selected.properties.value,7,'Select action uses the real node metadata');
  assert.equal(row.__nodeData,0,'the returned scalar remains data');
  assert.equal(row.nornicSelect,9);
});
await check('API_result_record',()=>{
  const result=input.results?.[0]??{columns:['__proto__','constructor',' ','a.b'],data:[{row:[7,8,9,10],meta:[]}]};
  const rows=api.parseCypherRows({results:[result]});
  for(let i=0;i<result.columns.length;i++){
    assert.ok(Object.hasOwn(rows[0],result.columns[i]));assert.deepEqual(rows[0][result.columns[i]],result.data[0].row[i]);
  }
});
for (const page of ['Cyber','Demo','Graphify']) await check(`${page}_result_record`,()=>{
  const project=load(`ui/src/pages/${page}.tsx`).testRowProjection;
  const row=project({results:[{columns:['__proto__',' ','constructor'],data:[{row:[7,8,9]}]}]})[0];
  assert.ok(Object.hasOwn(row,'__proto__'));assert.equal(row.__proto__,7);assert.equal(row[' '],8);
});
for (const transport of ['Bolt','HTTP']) await check(`${transport}_nested_property_serialization`,async()=>{
  const value=JSON.parse('{"__proto__":7,"nested":{"__proto__":8," ":9}}');
  api.fetchDiscovery=async()=>null;
  api.boltUnavailable=transport==='HTTP';
  api.getBoltDriver=async()=>({session:()=>({run:async()=>({records:[{keys:['value'],get:()=>value}]}),close:async()=>{}})});
  const previous=globalThis.fetch;
  globalThis.fetch=async()=>({ok:true,json:async()=>({results:[{columns:['value'],data:[{row:[value],meta:[null,null,null]}]}]})});
  try {
    const result=await api.executeCypherOnDatabase('nornic','RETURN $value',{value});
    assert.deepEqual(JSON.parse(JSON.stringify(result.results[0].data[0].row[0])),value);
  } finally {globalThis.fetch=previous;}
});
await check('legacy_node_property_record',()=>{
  const cell=JSON.parse('{"elementId":"4:nornicdb:n","__proto__":7,"constructor":8," ":9}');
  const node=nodeUtils.extractNodeFromResult(cell);
  assert.ok(Object.hasOwn(node.properties,'__proto__'));assert.equal(node.properties.__proto__,7);
  assert.deepEqual(JSON.parse(JSON.stringify(node.properties)),JSON.parse('{"__proto__":7,"constructor":8," ":9}'));
});
await check('property_editor_save',async()=>{
  const {PropertyEditor}=load('ui/src/components/common/PropertyEditor.tsx');
  let saved;
  const properties=JSON.parse('{"__proto__":7,"constructor":8," ":9}');
  const tree=render(PropertyEditor,{properties,onSave:async value=>{saved=value;},onCancel(){}});
  const save=elements(tree,e=>e.type==='button' && String(e.props.children).includes('Save'))[0];
  assert.ok(save,'actual Save button');await save.props.onClick();
  assert.deepEqual(JSON.parse(JSON.stringify(saved)),properties);
});
for (const key of ['__proto__',' ','  ','\t','a`b']) {
  for (const operation of ['add','rename']) await check(`property_editor_${operation}/${JSON.stringify(key)}`,async()=>{
    const {PropertyEditor}=load('ui/src/components/common/PropertyEditor.tsx');
    let saved;
    const props={properties:operation==='rename'?{original:7}:{},onSave:async value=>{saved=value;},onCancel(){}};
    let tree=render(PropertyEditor,props);
    const redraw=()=>{stateIndex=0;effects=[];tree=PropertyEditor(props);};
    const find=placeholder=>elements(tree,e=>e.props?.placeholder===placeholder)[0];
    if(operation==='rename'){
      find('Property key').props.onChange({target:{value:key}});
      redraw();
    } else {
      find('New property key').props.onChange({target:{value:key}});
      const value=elements(tree,e=>e.type==='textarea').at(-1);
      value.props.onChange({target:{value:'7'}});
      redraw();
      find('New property key').props.onKeyDown({key:'Enter',preventDefault(){}});
      redraw();
    }
    const save=elements(tree,e=>e.type==='button' && String(e.props.children).includes('Save'))[0];
    await save.props.onClick();
    assert.ok(Object.hasOwn(saved,key),`saved exact ${operation} key`);
    assert.equal(saved[key],operation==='add'?'7':7);
    assert.equal(Object.hasOwn(saved,'original'),false);
  });
}
await check('metadata_target_emission',()=>{
  for(const labels of [['a:b'],['A','B'],[' '],['\t'],['a`b'],['*']]){
    const target=editing.targetValue(labels,undefined,false);
    const expected=`(n:${labels.map(names.quoteName).join(':')})`;
    assert.equal(editing.buildTarget(target,'NODE'),expected);
  }
  for(const type of ['A|B',' ','a`b'])assert.equal(editing.buildTarget(editing.targetValue([],type,false),'EDGE'),`()-[r:${names.quoteName(type)}]-()`);
  assert.equal(editing.buildTarget('*','NODE'),'()');
  assert.equal(editing.validateTarget('','NODE'),false);
  assert.equal(editing.validateTarget('`unfinished','NODE'),false);
  assert.equal(editing.validateTarget('A:B','EDGE'),false);
});
await check('policy_name_and_option_emission',()=>{
  const value=`a\\b'"c`;
  const name=editing.quotePolicyName(value);
  commands.push({kind:'profile-name',name:value,query:`CREATE DECAY PROFILE ${name} OPTIONS {function:'exponential',scope:'NODE',scoreFrom:'CREATED'}`});
  const query=editing.buildProfileOptionAlter('DECAY','ui_anchor','scoreFromProperty',value);
  commands.push({kind:'anchor',key:value,query});
});

function fakeGrid() {
  const validators=new Map();
  return {validators,validate:{setValidator(name,factory){validators.set(name,factory());},
    async runValidators(row,column,value,old){return Object.keys(column.validators??{}).filter(name=>!validators.get(name)(old,value,row));},
    getErrorMessages(){return ['invalid'];}},edit:{on:{afterCellEdit(handler){this.handler=handler;return ()=>{};}}},core:{refreshRows(){}}};
}
await check('registered_anchor_editor',async()=>{
  const {KnowledgePolicyEditorGrids}=load('ui/src/components/knowledge/KnowledgePolicyEditorGrids.tsx');
  const grid=fakeGrid(),errors=[],statements=[];
  render(KnowledgePolicyEditorGrids,{section:'decay',profiles:{bundles:[],bindings:[]},policies:null,saving:false,
    onAlter:async statement=>{statements.push(statement);return true;},onValidationError:message=>errors.push(message)},[grid,null,null,null]);
  const validator=grid.validators.get('requiredForCustom');assert.ok(validator);
  assert.equal(validator('', '', {ScoreFrom:'CUSTOM'}),false);
  assert.equal(validator('', '', {ScoreFrom:'CREATED'}),true);
  for(const key of [' ','  ','\t',`a\\b'"c`]) {
    assert.equal(validator('old',key,{ScoreFrom:'CUSTOM'}),true);
    const row={Name:'ui_anchor',ScoreFrom:'CUSTOM',ScoreFromProperty:key};
    grid.edit.on.handler(row,{name:'ScoreFromProperty',field:'ScoreFromProperty',validators:{requiredForCustom:true}},key,'old');
    await new Promise(resolve=>setImmediate(resolve));
    assert.equal(errors.length,0);assert.equal(statements.length,commands.filter(c=>c.kind==='grid-anchor').length+1);
    commands.push({kind:'grid-anchor',key,query:statements.at(-1)});
  }
});
await check('metadata_grid_to_alter',async()=>{
  const {KnowledgePolicyEditorGrids}=load('ui/src/components/knowledge/KnowledgePolicyEditorGrids.tsx');
  const bindings=input.bindings??[{Name:'ui_binding',TargetLabels:['a:b'],IsEdge:false,IsWildcard:false,Apply:'NO DECAY'}];
  const tree=render(KnowledgePolicyEditorGrids,{section:'decay',profiles:{bundles:[],bindings},policies:null,saving:false,onAlter:async()=>true,onValidationError(){}},[null,null,null,null]);
  const grid=elements(tree,e=>e.type===UiGrid && e.props.options.id==='knowledge-decay-bindings')[0];assert.ok(grid);
  for(const row of grid.props.options.data)commands.push({kind:'binding',name:row.Name,labels:row.TargetLabels,edgeType:row.TargetEdgeType,query:editing.buildDecayBindingAlter(row)});
});
await check('API_resolve_parameter_identity',async()=>{
  api.getResolvedDatabaseName=async()=> 'nornic';
  let call;
  api.executeCypherOnDatabase=async(db,query,parameters)=>{call={db,query,parameters};return {results:[{columns:[],data:[{row:[]}]}]};};
  const labels=[' ','A,B']; await api.resolveKnowledgePolicy({labels,edgeType:' A|B '});
  assert.deepEqual(call.parameters,{entityId:'',labels,edgeType:' A|B '});
  commands.push({kind:'resolve',...call});
});
await check('database_access_column_namespace',async()=>{
  const {DatabaseAccess}=load('ui/src/pages/DatabaseAccess.tsx');
  const dbNames=input.databaseNames??['nornic','role','role_'];
  const previous=globalThis.fetch;
  api.listDatabases=async()=>dbNames.map(name=>({name}));
  const responses={
    '/auth/me':{roles:['admin']},'/auth/roles':['admin','viewer'],
    '/auth/access/databases':[], '/auth/access/privileges':[],
    '/auth/entitlements':[], '/auth/role-entitlements':[],
  };
  globalThis.fetch=async url=>({ok:true,json:async()=>responses[url]??[]});
  try {
    render(DatabaseAccess,{});
    await new Promise(resolve=>setImmediate(resolve));
    stateIndex=0;effects=[];
    const tree=DatabaseAccess({});
    const grid=elements(tree,e=>e.type===UiGrid && e.props.options.id==='database-access-grid')[0];
    assert.ok(grid,'actual admin data load populates grid');
    const options=grid.props.options;
    assert.equal(new Set(options.columnDefs.map(c=>c.name)).size,options.columnDefs.length);
    const column=options.columnDefs.find(c=>c.name==='role');assert.ok(column);
    const row=options.data.find(r=>r.role==='viewer');assert.ok(row);
    const cell=grid.props.cellRenderers.role({row,column});
    const checkbox=elements(cell,e=>e.type==='input' && e.props.type==='checkbox')[0];
    assert.ok(checkbox,'the database role uses a checkbox, independently of the fixed role-label cell');
    checkbox.props.onChange();
    const actualDatabases=options.columnDefs.filter(c=>c.field!=='role').map(c=>c.name);
    assert.deepEqual(states[5].find(r=>r.role==='viewer').databases,actualDatabases.filter(name=>name!=='role'));
  } finally {globalThis.fetch=previous;}
});
console.log(JSON.stringify({commands,failures,observations}));
if(failures.length)process.exitCode=1;
