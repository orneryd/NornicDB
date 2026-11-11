import {test} from 'node:test';
import assert from 'node:assert/strict';
import {decayBaseline,newDecayProfileStatement,newDecayBindingStatement} from '../src/utils/knowledgePolicyGuidance.ts';
import {schemaStatement,quoteIdentifier} from '../src/utils/schemaOperations.ts';
const profile={Name:'memory',Function:'exponential',HalfLifeSeconds:86400,VisibilityThreshold:0.1,ScoreFloor:0,Scope:'NODE',ScoreFrom:'VERSION',Enabled:true,DecayEnabled:true};
test('decay guidance follows forward and inverse scorer mathematics, floors and time anchors',()=>{
 assert.equal(decayBaseline(profile,0),1);assert.equal(decayBaseline(profile,86400),0.5);assert.equal(decayBaseline(profile,4*86400),0.0625);
 const inverse={...profile,HalfLifeSeconds:-86400,ScoreFrom:'LAST_ACCESSED',ScoreFloor:0.1};
 assert.equal(decayBaseline(inverse,0),0.1);assert.equal(decayBaseline(inverse,86400),0.5);assert.equal(decayBaseline(inverse,4*86400),0.9375);
 assert.equal(decayBaseline({...profile,Function:'linear'},2*86400),0);
 assert.equal(decayBaseline({...inverse,Function:'step'},86400),1);
 assert.equal(decayBaseline({...profile,Function:'none'},10*86400),1);
 assert.match(newDecayProfileStatement(inverse),/halfLifeSeconds: -86400/);
 assert.match(newDecayProfileStatement(inverse),/scoreFrom: 'LAST_ACCESSED'/);
 assert.throws(()=>newDecayProfileStatement({...profile,ScoreFloor:2}));
});
test('guided bindings use canonical node intersections and relationship targets',()=>{
 assert.equal(newDecayBindingStatement('episode_binding','NODE','Memory:Episode','memory'), 'CREATE DECAY PROFILE episode_binding FOR (n:Memory:Episode) APPLY { DECAY PROFILE memory }');
 assert.match(newDecayBindingStatement('edge_binding','EDGE','SUPPORTS','memory'),/FOR \(\)-\[r:SUPPORTS\]-\(\)/);
 assert.throws(()=>newDecayBindingStatement('binding','EDGE','*','memory'));
});
test('schema forms quote names and produce valid node and relationship statements',()=>{
 assert.equal(quoteIdentifier('odd`name'),'`odd``name`');
 assert.equal(schemaStatement({kind:'index',name:'by name',entity:'node',target:'Person',properties:['name'],constraint:'unique'}),'CREATE INDEX `by name` IF NOT EXISTS FOR (n:`Person`) ON (n.`name`)');
 assert.equal(schemaStatement({kind:'constraint',name:'email_unique',entity:'node',target:'Person',properties:['tenant','email'],constraint:'unique'}),'CREATE CONSTRAINT `email_unique` IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`tenant`, n.`email`) IS UNIQUE');
 assert.match(schemaStatement({kind:'constraint',name:'required',entity:'relationship',target:'SUPPORTS',properties:['source'],constraint:'exists'}),/FOR \(\)-\[r:`SUPPORTS`\]-\(\) REQUIRE r.`source` IS NOT NULL/);
 assert.throws(()=>schemaStatement({kind:'constraint',name:'invalid',entity:'node',target:'Person',properties:['a','b'],constraint:'exists'}));
});
