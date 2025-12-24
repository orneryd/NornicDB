package cypher_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNorthwindFastPaths(t *testing.T) {
	exec := testutil.SetupTestExecutor(t)

	// Minimal Northwind-like dataset.
	seed := []string{
		`CREATE (:Category {categoryID: 1, categoryName: 'Beverages'})`,
		`CREATE (:Category {categoryID: 2, categoryName: 'Condiments'})`,

		`CREATE (:Supplier {supplierID: 1, companyName: 'Exotic Liquids'})`,
		`CREATE (:Supplier {supplierID: 2, companyName: 'New Orleans Cajun Delights'})`,

		`MATCH (c:Category {categoryID: 1}) CREATE (p:Product {productID: 1, productName: 'Chai', unitPrice: 18.0})-[:PART_OF]->(c)`,
		`MATCH (c:Category {categoryID: 1}) CREATE (p:Product {productID: 2, productName: 'Chang', unitPrice: 19.0})-[:PART_OF]->(c)`,
		`MATCH (c:Category {categoryID: 2}) CREATE (p:Product {productID: 3, productName: 'Aniseed Syrup', unitPrice: 10.0})-[:PART_OF]->(c)`,
		`MATCH (c:Category {categoryID: 1}) CREATE (p:Product {productID: 4, productName: 'NoOrders', unitPrice: 5.0})-[:PART_OF]->(c)`,

		`MATCH (s:Supplier {supplierID: 1}), (p:Product {productID: 1}) CREATE (s)-[:SUPPLIES]->(p)`,
		`MATCH (s:Supplier {supplierID: 1}), (p:Product {productID: 3}) CREATE (s)-[:SUPPLIES]->(p)`,
		`MATCH (s:Supplier {supplierID: 2}), (p:Product {productID: 2}) CREATE (s)-[:SUPPLIES]->(p)`,

		`CREATE (:Customer {customerID: 'ALFKI', companyName: 'Alfreds Futterkiste'})`,
		`CREATE (:Customer {customerID: 'ANATR', companyName: 'Ana Trujillo Emparedados y helados'})`,

		`MATCH (c:Customer {customerID: 'ALFKI'}) CREATE (o:Order {orderID: 10643})<-[:PURCHASED]-(c)`,
		`MATCH (c:Customer {customerID: 'ALFKI'}) CREATE (o:Order {orderID: 10308})<-[:PURCHASED]-(c)`,

		`MATCH (o:Order {orderID: 10643}), (p:Product {productID: 1}) CREATE (o)-[:ORDERS {quantity: 5}]->(p)`,
		`MATCH (o:Order {orderID: 10643}), (p:Product {productID: 2}) CREATE (o)-[:ORDERS {quantity: 2}]->(p)`,
		`MATCH (o:Order {orderID: 10308}), (p:Product {productID: 3}) CREATE (o)-[:ORDERS {quantity: 3}]->(p)`,
	}
	for _, q := range seed {
		testutil.MustExecute(t, exec, q)
	}

	t.Run("Supplier to category through products", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)-[:PART_OF]->(c:Category)
			RETURN s.companyName, c.categoryName, count(p) as products
			ORDER BY products DESC
		`, nil)
		require.Equal(t, []string{"s.companyName", "c.categoryName", "products"}, res.Columns)
		require.Len(t, res.Rows, 3)

		got := map[[2]string]int64{}
		for _, r := range res.Rows {
			got[[2]string{r[0].(string), r[1].(string)}] = r[2].(int64)
		}
		assert.Equal(t, int64(1), got[[2]string{"Exotic Liquids", "Beverages"}])
		assert.Equal(t, int64(1), got[[2]string{"Exotic Liquids", "Condiments"}])
		assert.Equal(t, int64(1), got[[2]string{"New Orleans Cajun Delights", "Beverages"}])
	})

	t.Run("Customer to category through orders and products", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)
			RETURN c.companyName, cat.categoryName, count(DISTINCT o) as orders
			ORDER BY orders DESC
			LIMIT 10
		`, nil)
		require.Equal(t, []string{"c.companyName", "cat.categoryName", "orders"}, res.Columns)

		got := map[[2]string]int64{}
		for _, r := range res.Rows {
			got[[2]string{r[0].(string), r[1].(string)}] = r[2].(int64)
		}
		assert.Equal(t, int64(1), got[[2]string{"Alfreds Futterkiste", "Beverages"}])
		assert.Equal(t, int64(1), got[[2]string{"Alfreds Futterkiste", "Condiments"}])
	})

	t.Run("Customer to supplier through orders and products", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)<-[:SUPPLIES]-(s:Supplier)
			RETURN c.companyName, s.companyName, count(DISTINCT o) as orders
			ORDER BY orders DESC
			LIMIT 10
		`, nil)
		require.Equal(t, []string{"c.companyName", "s.companyName", "orders"}, res.Columns)

		// Highest should be Exotic Liquids with 2 orders.
		require.GreaterOrEqual(t, len(res.Rows), 1)
		assert.Equal(t, "Alfreds Futterkiste", res.Rows[0][0])
		assert.Equal(t, "Exotic Liquids", res.Rows[0][1])
		assert.Equal(t, int64(2), res.Rows[0][2])
	})

	t.Run("Products per category", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (c:Category)<-[:PART_OF]-(p:Product)
			RETURN c.categoryName, count(p) as productCount
			ORDER BY productCount DESC
		`, nil)
		require.Equal(t, []string{"c.categoryName", "productCount"}, res.Columns)
		require.Len(t, res.Rows, 2)
		assert.Equal(t, "Beverages", res.Rows[0][0])
		assert.Equal(t, int64(3), res.Rows[0][1])
	})

	t.Run("Average price per category", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (c:Category)<-[:PART_OF]-(p:Product)
			RETURN c.categoryName, avg(p.unitPrice) as avgPrice, count(p) as products
			ORDER BY avgPrice DESC
		`, nil)
		require.Equal(t, []string{"c.categoryName", "avgPrice", "products"}, res.Columns)
		require.Len(t, res.Rows, 2)
		assert.Equal(t, "Beverages", res.Rows[0][0])
		assert.InDelta(t, 14.0, res.Rows[0][1].(float64), 0.0001)
		assert.Equal(t, int64(3), res.Rows[0][2])
	})

	t.Run("Total quantity ordered per product", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (p:Product)<-[r:ORDERS]-(:Order)
			RETURN p.productName, sum(r.quantity) as totalOrdered
			ORDER BY totalOrdered DESC
			LIMIT 10
		`, nil)
		require.Equal(t, []string{"p.productName", "totalOrdered"}, res.Columns)
		require.GreaterOrEqual(t, len(res.Rows), 1)
		assert.Equal(t, "Chai", res.Rows[0][0])
		assert.Equal(t, float64(5), res.Rows[0][1])
	})

	t.Run("Orders per customer", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (c:Customer)-[:PURCHASED]->(o:Order)
			RETURN c.companyName, count(o) as orderCount
			ORDER BY orderCount DESC
		`, nil)
		require.Equal(t, []string{"c.companyName", "orderCount"}, res.Columns)
		require.GreaterOrEqual(t, len(res.Rows), 1)
		assert.Equal(t, "Alfreds Futterkiste", res.Rows[0][0])
		assert.Equal(t, int64(2), res.Rows[0][1])
	})

	t.Run("Products per supplier", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)
			RETURN s.companyName, count(p) as productCount
			ORDER BY productCount DESC
		`, nil)
		require.Equal(t, []string{"s.companyName", "productCount"}, res.Columns)
		require.GreaterOrEqual(t, len(res.Rows), 1)
		assert.Equal(t, "Exotic Liquids", res.Rows[0][0])
		assert.Equal(t, int64(2), res.Rows[0][1])
	})

	t.Run("Top products by revenue", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (p:Product)<-[r:ORDERS]-(:Order)
			WITH p, sum(p.unitPrice * r.quantity) as revenue
			RETURN p.productName, revenue
			ORDER BY revenue DESC
			LIMIT 10
		`, nil)
		require.Equal(t, []string{"p.productName", "revenue"}, res.Columns)
		require.GreaterOrEqual(t, len(res.Rows), 1)
		assert.Equal(t, "Chai", res.Rows[0][0])
		assert.InDelta(t, 90.0, res.Rows[0][1].(float64), 0.0001)
	})

	t.Run("Categories with product lists", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (c:Category)<-[:PART_OF]-(p:Product)
			RETURN c.categoryName, collect(p.productName) as products
		`, nil)
		require.Equal(t, []string{"c.categoryName", "products"}, res.Columns)
		require.Len(t, res.Rows, 2)

		got := map[string][]interface{}{}
		for _, r := range res.Rows {
			got[r[0].(string)] = r[1].([]interface{})
		}
		assert.Len(t, got["Beverages"], 3)
	})

	t.Run("Customers with order lists", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (c:Customer)-[:PURCHASED]->(o:Order)
			RETURN c.companyName, collect(o.orderID) as orders
		`, nil)
		require.Equal(t, []string{"c.companyName", "orders"}, res.Columns)
		require.GreaterOrEqual(t, len(res.Rows), 1)
		orders := res.Rows[0][1].([]interface{})
		assert.Len(t, orders, 2)
	})

	t.Run("Products with or without orders (OPTIONAL MATCH)", func(t *testing.T) {
		res := testutil.ExecuteQuery(t, exec, `
			MATCH (p:Product)
			OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
			RETURN p.productName, count(o) as orderCount
			ORDER BY orderCount DESC
		`, nil)
		require.Equal(t, []string{"p.productName", "orderCount"}, res.Columns)
		require.Len(t, res.Rows, 4)
		// NoOrders should have 0 and end up last when ordering by count desc.
		last := res.Rows[len(res.Rows)-1]
		assert.Equal(t, "NoOrders", last[0])
		assert.Equal(t, int64(0), last[1])
	})

	t.Run("Create and delete relationship (fast no-op)", func(t *testing.T) {
		before := testutil.ExecuteQuery(t, exec, `MATCH ()-[r]->() RETURN count(r)`, nil)
		require.Len(t, before.Rows, 1)

		res := testutil.ExecuteQuery(t, exec, `
			MATCH (s:Supplier {supplierID: 1}), (p:Product {productID: 1})
			CREATE (s)-[r:TEST_REL]->(p)
			WITH r
			DELETE r
			RETURN count(r)
		`, nil)
		require.Equal(t, []string{"count(r)"}, res.Columns)
		require.Equal(t, int64(1), res.Rows[0][0])

		after := testutil.ExecuteQuery(t, exec, `MATCH ()-[r]->() RETURN count(r)`, nil)
		require.Equal(t, before.Rows[0][0], after.Rows[0][0], "fast-path should not persist TEST_REL")
	})
}
