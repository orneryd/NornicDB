import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
import {checkAccessibility} from './accessibility.mjs';
import {mkdir} from 'node:fs/promises';

async function codeFixture(page,{named=true}={}){
 const state=await fixture(page);const queries=[],seeds=[];
 const symbols=[['entry','main','cmd/server/main.go','func main() { startServer() }'],['server','startServer','internal/server.go','func startServer() { route() }'],['route','route','internal/routes.go','func route() { search() }'],['search','search','internal/search.go','func search() {}'],['utility','parse','internal/parse.go','func parse() {}']];
 const records=rows=>{const columns=Object.keys(rows[0]??{});return {results:[{columns,data:rows.map(row=>({row:columns.map(column=>row[column])}))}],errors:[]};};
 await page.route('**/db/*/tx/commit',async route=>{
  const statement=route.request().postDataJSON().statements[0],query=statement.statement,parameters=statement.parameters??{};
  if(query.includes('SHOW DATABASES'))return route.fallback();
  queries.push(statement);let rows=[];
  if(query.includes('MATCH (a:Main)'))rows=[];
  else if(query.includes('toLower(a.label)'))rows=named?[{id:'entry',label:'main',source_file:'cmd/server/main.go'}]:[];
  else if(query.includes('id(a) AS internalId'))rows=[{id:'utility',internalId:'utility',label:'parse',source_file:'internal/parse.go'}];
  else if(query.includes('AS internalId'))rows=[{internalId:parameters.graphifyId}];
  else if(query.includes('n.body AS body'))rows=[{body:symbols.find(node=>node[0]===parameters.id)?.[3]}];
  return route.fulfill({contentType:'application/json',body:JSON.stringify(records(rows))});
 });
 await page.route('**/neighborhood',async route=>{
  const body=route.request().postDataJSON();seeds.push(body);
  return route.fulfill({contentType:'application/json',body:JSON.stringify({nodes:symbols.map(([id,label,source_file,body])=>({id,labels:['Code'],properties:{id,label,source_file,body,file_type:'go',node_kind:'function'}})),edges:[['entry','server'],['server','route'],['route','search'],['route','utility']].map(([source,target],i)=>({id:String(i),source,target,type:'CALLS',properties:{}})),meta:{database:'catalog',node_count:5,edge_count:4,truncated:false}})});
 });
 await page.route('**/nornicdb/search',route=>route.fulfill({contentType:'application/json',body:JSON.stringify({results:[{node:{id:'search',properties:{id:'search',label:'search',source_file:'internal/search.go'}},score:.9}]})}));
 return {state,queries,seeds};
}

test('suggested entry, keyboard inspection and URL restoration',async({page},testInfo)=>{
 const {seeds}=await codeFixture(page);const errors=[];page.on('pageerror',error=>errors.push(error.message));
 await page.goto('/graphify?database=catalog');
 await expect(page.getByRole('heading',{name:'Explore how a project connects'})).toBeVisible();
 await page.getByRole('button',{name:'Load tree',exact:true}).click();
 await expect(page.getByTestId('graphify-details')).toContainText('func main()');
 expect(seeds[0].node_ids).toEqual(['entry']);
 await expect(page.getByLabel('Starting point')).toHaveValue('entry');
 await mkdir('review/graphify',{recursive:true});await page.screenshot({path:'review/graphify/overview.png',fullPage:true});
 await expect(page.locator('.graphify-entry')).toContainText('Conventional executable entry');
 await expect(page.getByText('Advanced filters',{exact:true})).toBeVisible();
 expect(await page.locator('.graphify-advanced').evaluate(el=>el.open)).toBe(false);
 await page.getByText('Advanced filters',{exact:true}).click();await expect(page).toHaveURL(/advanced=true/);
 await page.getByRole('button',{name:'Browse symbols',exact:true}).click();
 await page.getByRole('region',{name:'Loaded symbols'}).getByRole('button',{name:'startServer'}).click();
 await expect(page.getByTestId('graphify-details')).toContainText('func startServer()');await expect(page).toHaveURL(/node=/);
 await page.getByTestId('graphify-details').getByText('Source code',{exact:true}).click();await expect(page).toHaveURL(/sourceBody=false/);
 await page.reload();
 await expect(page.getByTestId('graphify-details')).toContainText('startServer');
 expect(await page.getByTestId('graphify-details').locator('details').first().evaluate(el=>el.open)).toBe(false);
 await page.getByRole('button',{name:'Graph controls',exact:true}).click();await expect(page).toHaveURL(/help=true/);
 await page.getByRole('button',{name:'Float inspector',exact:true}).click();await expect(page).toHaveURL(/floating=true/);
 await page.getByRole('button',{name:'Dock inspector',exact:true}).click();
 await checkAccessibility(page,testInfo);
 expect(errors).toEqual([]);
 await mkdir('review/graphify',{recursive:true});await page.screenshot({path:'review/graphify/desktop.png',fullPage:true});
});

test('search URL survives reload and import is inline with an explicit target',async({page})=>{
 await codeFixture(page);await page.goto('/graphify?database=catalog&q=search');
 await expect(page.getByLabel('Find a symbol or source file',{exact:true})).toHaveValue('search');
 await expect(page.getByRole('button',{name:/search.*internal\/search.go/})).toBeVisible();
 await page.getByRole('button',{name:'upload graphify artifact',exact:true}).click();
 await expect(page).toHaveURL(/upload=true/);
 await expect(page.getByRole('region',{name:'Import Graphify artifact'})).toBeVisible();
 await page.getByLabel('Graphify artifact file').setInputFiles({name:'graph.json',mimeType:'application/json',buffer:Buffer.from(JSON.stringify({nodes:[{id:'entry',label:'main',file_type:'go',source_file:'cmd/server/main.go',node_kind:'function'}],links:[]}))});
 await page.getByLabel('database name',{exact:true}).fill('catalog');
 await expect(page.getByRole('button',{name:'ingest into database',exact:true})).toBeDisabled();
 await page.getByRole('checkbox',{name:/update existing database/}).check();
 await page.getByRole('button',{name:'ingest into database',exact:true}).click();
 await expect(page.getByRole('region',{name:'Import Graphify artifact'})).toHaveCount(0);
 await expect(page.getByTestId('graphify-details')).toContainText('main');
});

test('mobile layout keeps controls reachable without horizontal overflow',async({page},testInfo)=>{
 await codeFixture(page);await page.setViewportSize({width:390,height:844});
 await page.goto('/graphify?database=catalog&loaded=true');await expect(page.getByTestId('graphify-details')).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 await checkAccessibility(page,testInfo);
 await mkdir('review/graphify',{recursive:true});await page.screenshot({path:'review/graphify/mobile.png',fullPage:true});
});

test('unmarked library uses a bounded neighborhood sample rather than a whole-graph expansion',async({page})=>{
 const {queries,seeds}=await codeFixture(page,{named:false});
 await page.goto('/graphify?database=catalog&loaded=true');
 await expect(page.getByTestId('graphify-details')).toContainText('parse');
 expect(seeds[0].node_ids).toEqual(['utility']);expect(seeds[0].limit).toBe(2000);expect(seeds[0].depth).toBe(1);
 expect(queries.some(query=>query.statement.includes('count(DISTINCT b)'))).toBe(false);
});
