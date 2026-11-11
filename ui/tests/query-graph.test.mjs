import {test} from 'node:test';
import assert from 'node:assert/strict';
import {graphFromQuery} from '../src/utils/queryGraph.ts';
const node=id=>({elementId:`4:catalog:${id}`,labels:['Document'],properties:{title:id}});
const edge={elementId:'5:catalog:edge',type:'LINKS',startNodeElementId:'4:catalog:a',endNodeElementId:'4:catalog:b',properties:{weight:2}};
const result=row=>({results:[{columns:['value'],data:[{row,meta:[]}]}]});
test('query graph extracts nested nodes, relationships and paths without turning scalar maps into nodes',()=>{
 const graph=graphFromQuery(result([{path:[node('a'),edge,node('b'),node('a')]},{id:'scalar',value:3}]),'catalog');
 assert.deepEqual(graph.nodes.map(n=>n.id),['a','b']);assert.equal(graph.edges[0].source,'a');assert.equal(graph.edges[0].target,'b');assert.equal(graph.edges[0].properties.weight,2);
 assert.equal(graph.meta.truncated,false);
});
test('relationship-only results retain endpoint placeholders and bounded graphs report truncation',()=>{
 const graph=graphFromQuery(result([edge]),'catalog');assert.equal(graph.nodes.length,2);assert.equal(graph.nodes[0].labels[0],'Endpoint');
 const limited=graphFromQuery(result([node('a'),node('b'),edge]),'catalog',1);assert.equal(limited.nodes.length,1);assert.equal(limited.edges.length,0);assert.equal(limited.meta.truncated,true);
 assert.equal(graphFromQuery(null,'catalog').nodes.length,0);
});
