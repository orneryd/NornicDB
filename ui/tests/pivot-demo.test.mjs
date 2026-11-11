import {test} from 'node:test';
import assert from 'node:assert/strict';
import {aggregateSales,pivotQuery,projectPivot,sampleSales} from '../src/utils/pivotDemo.ts';
test('pivot preserves totals across dimensions and distinguishes counts, units, and revenue',()=>{
 const sales=sampleSales(500);
 for(const measure of ['orders','units','revenue']){
  const groups=aggregateSales(sales,'region','quarter',measure);
  const pivot=projectPivot(groups,false);
  const expected=measure==='orders'?500:sales.reduce((total,sale)=>total+sale[measure],0);
  assert.equal(pivot.rows.reduce((total,row)=>total+row.total,0),expected);
  assert.equal(groups.reduce((total,group)=>total+group.records,0),500);
 }
});
test('average totals weight groups by the number of contributing orders',()=>{
 const result=projectPivot([{rowKey:'Europe',columnKey:'Q1',amount:200,records:1},{rowKey:'Europe',columnKey:'Q2',amount:900,records:9}],true);
 assert.equal(result.rows[0].Q1,200);assert.equal(result.rows[0].Q2,100);assert.equal(result.rows[0].total,110);
 assert.deepEqual(projectPivot([],false),{columns:[],rows:[]});
});
test('query uses only supported fields and seeded demo records without database mutations',()=>{
 assert.match(pivotQuery('region','quarter','revenue'),/sum\(sale.revenue\)/);
 assert.match(pivotQuery('product','channel','orders'),/count\(\*\) AS amount/);
 assert.throws(()=>pivotQuery('unsafe','quarter','revenue'));
 assert.doesNotMatch(pivotQuery('region','quarter','revenue'),/CREATE|MERGE|DELETE|SET/);
});

test('seed dataset supplies enough distinct rows for virtualization',()=>{
 const sales=sampleSales(25000);
 assert.equal(sales.length,25000);
 assert.equal(projectPivot(aggregateSales(sales,'account','quarter','revenue'),false).rows.length,1000);
});

test('calculated cells and totals use weighted ratios and dataset revenue shares',()=>{
 const groups=[{rowKey:'A',columnKey:'Q1',amount:1000,records:1,revenue:1000,units:2},{rowKey:'A',columnKey:'Q2',amount:6000,records:3,revenue:6000,units:20},{rowKey:'B',columnKey:'Q1',amount:3000,records:2,revenue:3000,units:0}];
 const ratio=projectPivot(groups,false,'revenuePerUnit');
 assert.equal(ratio.rows[0].Q1,500);assert.equal(ratio.rows[0].Q2,300);
 assert.equal(ratio.rows[0].total,7000/22);assert.equal(ratio.rows[1].total,null);
 const share=projectPivot(groups,false,'revenueShare');
 assert.equal(share.rows[0].Q1,0.1);assert.equal(share.rows[0].total,0.7);assert.equal(share.rows[1].total,0.3);
});
