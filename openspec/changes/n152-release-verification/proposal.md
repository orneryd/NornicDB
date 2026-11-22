# Refresh release verification inventories

The v1.5.2 release CI utility group fails because the autocomplete handler still uses the same typed JSON boundary, but its reviewed fingerprint predates the model framing changes in #1933. A separate lexical inventory stops at a DELETE helper removed by #1921.

Update the autocomplete fingerprint after checking the unchanged JSON calls and request fields. Refresh the lexical inventory from the current owners. Remove retired rows, retain the three moved traversal entries with their existing evidence, and review every added or changed classification. The collector keeps the scope of its test population. The autocomplete entry references an in-scope test for generation stops while the bundled plugin verification still runs separately.

Neither checker changes. The exact release source reproduced both failures, and the corrected inventories pass the owning packages.

- [x] Reproduce both gates on release head `f2e264f1f02f615ccbf9be498ed94993448dcb9d`.
- [x] Reconcile the typed JSON exception and reviewed lexical population against current code.
- [x] Run both owning package suites.
- [ ] Run candidate release utility and verification checks and publish their status.
