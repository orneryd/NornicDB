import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';

test('assistant launchers use flat styling and reflect server availability',async({page})=>{
 await fixture(page);
 let enabled=false,failed=false;
 await page.route('**/api/bifrost/status',route=>route.fulfill({status:failed?503:200,contentType:'application/json',body:JSON.stringify({model:'qwen3.5-0.8b',heimdall:{enabled}})}));
 await page.clock.install();
 await page.goto('/workspace/catalog');
 const header=page.locator('header').getByRole('button',{name:'AI Assistant',exact:true});
 const navigation=page.locator('nav').getByRole('button',{name:'AI assistant',exact:true});
 await expect(header).toBeDisabled();await expect(navigation).toBeDisabled();
 await expect(header).toHaveAttribute('title','AI assistant unavailable');
 expect(await header.evaluate(el=>getComputedStyle(el).borderRadius)).toBe('3px');
 enabled=true;await page.clock.runFor(15000);
 await expect(header).toBeEnabled();await expect(navigation).toBeEnabled();
 await header.click();await expect(page).toHaveURL(/assistant=open/);
 failed=true;await page.clock.runFor(15000);
 await expect(header).toBeDisabled();await expect(navigation).toBeDisabled();
});
