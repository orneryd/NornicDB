import AxeBuilder from '@axe-core/playwright';
import {expect} from '@playwright/test';

/** Exact, operator-accepted upstream defect; all other findings still fail. */
export async function checkAccessibility(page,testInfo,selector){
 const builder=new AxeBuilder({page}).withTags(['wcag2a','wcag2aa','wcag21aa']);
 if(selector)builder.include(selector);
 const report=await builder.analyze();
 const known=[],unexpected=[];
 for(const violation of report.violations){
  if(violation.id==='aria-required-children'&&violation.nodes.every(node=>(node.html.startsWith('<section class="grid-frame ui-grid"')&&node.html.includes('role="grid"'))||(node.html.startsWith('<div class="pin-menu"')&&node.html.includes('role="menu"'))))known.push(violation);
  else unexpected.push(violation);
 }
 if(known.length)await testInfo.attach('known-ui-grid-7411',{body:JSON.stringify({issue:'https://github.com/orneryd/ui-grid/issues/7411',violations:known},null,2),contentType:'application/json'});
 expect(unexpected.map(v=>({id:v.id,nodes:v.nodes.map(n=>({html:n.html,summary:n.failureSummary}))}))).toEqual([]);
}
