import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';
test('database names filter through URLs; favorites persist locally per account',async({page})=>{
 await fixture(page);await page.goto('/databases');
 const favorite=page.getByRole('button',{name:'Favorite research',exact:true});await expect(favorite).toBeEnabled();await favorite.click();await expect(favorite).toHaveAttribute('aria-pressed','true');
 await expect(page.locator('.database-record h3').first()).toHaveText('research');
 await page.reload();await expect(favorite).toHaveAttribute('aria-pressed','true');
 const search=page.getByRole('searchbox',{name:'Filter databases by name'});await search.fill('CAT');await expect(page.getByRole('status')).toContainText('1 of 2 databases');await expect(page).toHaveURL(/databaseName=CAT/);await expect(page.locator('.database-record h3')).toHaveText('catalog');
 const favoritesOnly=page.getByRole('checkbox',{name:'Favorites only'});
 await page.reload();await expect(search).toHaveValue('CAT');
 await favoritesOnly.click();await expect(page).toHaveURL(/favoritesOnly=true/);await expect(favoritesOnly).toBeChecked();await expect(page.getByText('No databases match these filters.',{exact:false})).toBeVisible();
 await page.getByRole('button',{name:'Clear filters',exact:true}).click();await expect(favoritesOnly).not.toBeChecked();await favoritesOnly.click();await expect(favoritesOnly).toBeChecked();await expect(page.locator('.database-record h3')).toHaveText('research');
 await page.getByRole('button',{name:'Clear filters',exact:true}).click();
 await page.screenshot({path:'review/prototype/26-database-favorites.png',fullPage:false});
 await page.route('**/auth/me',route=>route.fulfill({json:{username:'sam',roles:['admin']}}));await page.reload();await expect(favorite).toHaveAttribute('aria-pressed','false');
 await page.setViewportSize({width:390,height:844});await expect(search).toBeVisible();expect(await page.evaluate(()=>document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
 await page.screenshot({path:'review/prototype/27-database-filter-mobile.png',fullPage:false});
});
