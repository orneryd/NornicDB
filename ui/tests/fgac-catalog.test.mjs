import {test} from 'node:test';
import assert from 'node:assert/strict';
import {
  buildCatalogBody,
  catalogPutBody,
  catalogSignature,
  definitionInput,
  describeAction,
  describeScope,
  inputForAction,
  normalizeCatalog,
  parseSelectors,
  removeDefinition,
  toggleRoleAssignment,
  upsertDefinition,
  validateCatalog,
  validateDefinitionInput,
} from '../src/utils/fgacCatalog.ts';

const actions=[
  {id:'database.access',resourceKind:'database'},
  {id:'graph.node.traverse',resourceKind:'node'},
  {id:'graph.node.property.read',resourceKind:'node',property:true},
  {id:'graph.node.create',resourceKind:'node',creates:true},
  {id:'graph.relationship.traverse',resourceKind:'relationship'},
  {id:'admin.command',resourceKind:'dbms'},
];

const emptyInput=(overrides={})=>({
  id:'reviewer_read',action:'graph.node.property.read',databaseId:'db-1',
  types:'Person',entityIds:'doc-1',properties:'title',deny:false,...overrides,
});

test('an empty catalog resolves to an editable draft and round-trips unchanged',()=>{
  assert.deepEqual(normalizeCatalog(null),{definitions:[],assignments:{}});
  assert.deepEqual(normalizeCatalog({definitions:null,assignments:null}),{definitions:[],assignments:{}});
  const stored={definitions:[{id:'traverse',action:'graph.node.traverse',scope:{kind:'node',databaseId:'db-1',types:['Person']}}],assignments:{analyst:['traverse']}};
  const draft=normalizeCatalog(stored);
  assert.equal(catalogSignature(draft),catalogSignature(normalizeCatalog(stored)));
  assert.deepEqual(buildCatalogBody(draft),stored);
  assert.deepEqual(buildCatalogBody({definitions:[],assignments:{}}),{definitions:[],assignments:{}});
  assert.equal(catalogSignature({definitions:[],assignments:{analyst:[],editor:[]}}),catalogSignature({definitions:[],assignments:{}}));
  assert.equal(catalogSignature({definitions:[],assignments:{analyst:['b','a']}}),catalogSignature({definitions:[],assignments:{analyst:['a','b']}}));
});

test('server catalog clones keep assignment arrays independent of the response',()=>{
  const stored={definitions:[],assignments:{analyst:['one']}};
  const draft=normalizeCatalog(stored);
  draft.assignments.analyst.push('two');
  assert.deepEqual(stored.assignments.analyst,['one']);
});

test('selector fields parse comma separated values without blanks or repeats',()=>{
  assert.deepEqual(parseSelectors(' Person, Company ,,Person '),['Person','Company']);
  assert.deepEqual(parseSelectors(''),[]);
  assert.equal(definitionInput().types,'');
});

test('definition input follows the selected action contract',()=>{
  const base=emptyInput();
  const asDbms=inputForAction(base,actions[5]);
  assert.equal(asDbms.databaseId,'');
  assert.equal(asDbms.properties,'');
  assert.equal(asDbms.types,'');
  const asCreate=inputForAction({...base,entityIds:'doc-1',types:'Person'},actions[3]);
  assert.equal(asCreate.entityIds,'');
  assert.equal(asCreate.types,'Person');
  const asDatabase=inputForAction({...base,types:'Person',entityIds:'doc-1'},actions[0]);
  assert.equal(asDatabase.types,'');
  assert.equal(asDatabase.entityIds,'');
  assert.equal(asDatabase.databaseId,'db-1');
});

test('definition validation mirrors the stored catalog rules',()=>{
  const valid=validateDefinitionInput(emptyInput(),actions,[],undefined);
  assert.deepEqual(valid.errors,[]);
  assert.deepEqual(valid.definition,{
    id:'reviewer_read',action:'graph.node.property.read',
    scope:{kind:'node',databaseId:'db-1',types:['Person'],entityIds:['doc-1'],properties:['title']},
  });
  const allDatabases=validateDefinitionInput(emptyInput({databaseId:'',types:'',entityIds:'',properties:''}),actions,[]);
  assert.deepEqual(allDatabases.definition?.scope,{kind:'node'});
  assert.deepEqual(validateDefinitionInput(emptyInput({id:'Bad'}),actions,[]).errors,
    ['Definition IDs must start with a lowercase letter and use only a-z, 0-9, "_", ".", "-".']);
  assert.deepEqual(validateDefinitionInput(emptyInput({id:''}),actions,[]).errors,['Enter a definition ID.']);
  assert.deepEqual(validateDefinitionInput(emptyInput({action:''}),actions,[]).errors,['Select an action.']);
  assert.deepEqual(validateDefinitionInput(emptyInput({action:'graph.node.unknown'}),actions,[]).errors,['Select an action.']);
  assert.deepEqual(validateDefinitionInput(emptyInput(),actions,[{id:'reviewer_read',action:'database.access',scope:{kind:'database'}}]).errors,
    ['A definition with ID "reviewer_read" already exists.']);
  assert.deepEqual(validateDefinitionInput(emptyInput(),actions,[{id:'reviewer_read',action:'database.access',scope:{kind:'database'}}],'reviewer_read').errors,[]);
  assert.deepEqual(validateDefinitionInput(emptyInput({action:'graph.node.create',entityIds:'doc-1',properties:''}),actions,[]).errors,
    ['Create actions cannot select entity IDs.']);
  assert.deepEqual(validateDefinitionInput(emptyInput({databaseId:''}),actions,[]).errors,
    ['Entity IDs require a specific database.']);
  assert.deepEqual(validateDefinitionInput(emptyInput({properties:'title',action:'graph.node.traverse'}),actions,[]).errors,
    ['Only property actions select properties.']);
  assert.deepEqual(validateDefinitionInput(emptyInput({types:'Person',properties:'',action:'database.access'}),actions,[]).errors,
    ['Only node and relationship actions select labels, types or entity IDs.']);
  assert.deepEqual(validateDefinitionInput(emptyInput({databaseId:'db-1',types:'',entityIds:'',properties:'',action:'admin.command'}),actions,[]).errors,
    ['A DBMS action applies to the whole DBMS and cannot select a database.']);
});

test('the PUT body carries the draft and the revision that was read',()=>{
  assert.deepEqual(catalogPutBody({definitions:[],assignments:{}},''),{catalog:{definitions:[],assignments:{}},revision:''});
  const definition={id:'traverse',action:'graph.node.traverse',scope:{kind:'node',databaseId:'db-1'},deny:true};
  const body=catalogPutBody({definitions:[definition],assignments:{analyst:['traverse','traverse']}},'rev-1');
  assert.deepEqual(body,{catalog:{definitions:[definition],assignments:{analyst:['traverse']}},revision:'rev-1'});
  assert.equal('deny' in catalogPutBody({definitions:[{...definition,deny:false}],assignments:{}},'r').catalog.definitions[0],false);
  assert.equal(JSON.parse(JSON.stringify(body)).catalog.definitions[0].scope.databaseId,'db-1');
});

test('denying a definition stores the deny flag only when checked',()=>{
  assert.equal(validateDefinitionInput(emptyInput(),actions,[]).definition?.deny,undefined);
  assert.equal(validateDefinitionInput(emptyInput({deny:true}),actions,[]).definition?.deny,true);
  assert.equal(definitionInput({id:'x',action:'database.access',scope:{kind:'database'},deny:true}).deny,true);
});

test('editing and removing definitions keeps role references consistent',()=>{
  let draft={definitions:[],assignments:{}};
  const added=validateDefinitionInput(emptyInput(),actions,[]).definition;
  draft=upsertDefinition(draft,added);
  draft=upsertDefinition(draft,validateDefinitionInput(emptyInput({id:'second',properties:'body'}),actions,draft.definitions).definition);
  assert.deepEqual(draft.definitions.map(definition=>definition.id),['reviewer_read','second']);
  const replaced=validateDefinitionInput(emptyInput({properties:'title, body'}),actions,draft.definitions,'reviewer_read').definition;
  assert.deepEqual(upsertDefinition(draft,replaced).definitions[0].scope.properties,['title','body']);
  assert.equal(upsertDefinition(draft,replaced).definitions.length,2);
  const assigned={definitions:draft.definitions,assignments:{analyst:['reviewer_read','second'],viewer:['reviewer_read']}};
  const toggled=toggleRoleAssignment(assigned.assignments,'analyst','second');
  assert.deepEqual(toggled,{analyst:['reviewer_read'],viewer:['reviewer_read']});
  assert.deepEqual(toggleRoleAssignment(toggled,'analyst','reviewer_read'),{viewer:['reviewer_read']});
  assert.deepEqual(toggleRoleAssignment({},'analyst','reviewer_read'),{analyst:['reviewer_read']});
  const removed=removeDefinition(assigned,'reviewer_read');
  assert.deepEqual(removed.definitions.map(definition=>definition.id),['second']);
  assert.deepEqual(removed.assignments,{analyst:['second']});
});

test('draft validation rejects unknown actions, roles and definitions',()=>{
  const draft=normalizeCatalog({definitions:[{id:'traverse',action:'graph.node.traverse',scope:{kind:'node'}}],assignments:{analyst:['traverse']}});
  assert.deepEqual(validateCatalog(draft,actions,['admin','analyst']),[]);
  assert.deepEqual(validateCatalog(draft,actions,['admin']),
    ['Role "analyst" does not exist; refresh the role list before saving.']);
  assert.deepEqual(validateCatalog({definitions:[{id:'traverse',action:'graph.node.gone',scope:{kind:'node'}}],assignments:{}},
    actions,['admin']),['Definition "traverse" uses unknown action "graph.node.gone".']);
  assert.deepEqual(validateCatalog({definitions:[],assignments:{analyst:['traverse']}},actions,['analyst']),
    ['Role "analyst" references missing definition "traverse".']);
  assert.deepEqual(validateCatalog({definitions:[],assignments:{analyst:['a','a']}},actions,['analyst']),
    ['Role "analyst" references missing definition "a".',
     'Role "analyst" references missing definition "a".',
     'Role "analyst" repeats a definition assignment.']);
  assert.deepEqual(validateCatalog({definitions:[],assignments:{'  ':['a']}},actions,[]),['A role assignment has no role name.']);
  assert.deepEqual(validateCatalog({definitions:[{id:'Bad',action:'database.access',scope:{kind:'database'}},{id:'Bad',action:'database.access',scope:{kind:'database'}}],assignments:{}},actions,[]),
    ['Definition "Bad" has an invalid ID.','Definition "Bad" has an invalid ID.','Definition "Bad" appears more than once.']);
});

test('scope and action descriptions name the stable database and selectors',()=>{
  const names={'db-1':'nornic'};
  assert.equal(describeScope({id:'a',action:'database.access',scope:{kind:'database',databaseId:'db-1'}},names),'nornic');
  assert.equal(describeScope({id:'a',action:'database.access',scope:{kind:'database'}},names),'All databases');
  assert.equal(describeScope({id:'a',action:'admin.command',scope:{kind:'dbms'}},names),'DBMS-wide');
  assert.equal(describeScope({id:'a',action:'graph.node.traverse',scope:{kind:'node',types:['Person','Company']}},names),
    'All databases · Person, Company');
  assert.equal(describeScope({id:'a',action:'graph.relationship.property.read',scope:{kind:'relationship',databaseId:'gone-1',properties:['weight']}},names),
    'gone-1 · all relationship types · properties: weight');
  assert.equal(describeScope({id:'a',action:'graph.node.traverse',scope:{kind:'node',databaseId:'db-1',entityIds:['doc-1']}},names),
    'nornic · all labels · entities: doc-1');
  assert.equal(describeAction({id:'graph.node.property.read',resourceKind:'node',property:true}),'node · property');
  assert.equal(describeAction({id:'graph.node.create',resourceKind:'node',creates:true}),'node · creates');
  assert.equal(describeAction({id:'database.access',resourceKind:'database'}),'database');
});
