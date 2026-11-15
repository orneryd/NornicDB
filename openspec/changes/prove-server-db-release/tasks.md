# Prove server/database shutdown ownership

- [x] Trace the completed server retirement and independent DB retirement owners.
- [x] Check that plugin completion precedes exactly one server DB-ticket release.
- [x] Assert release before successful WaitStopped, without timing physical database close.
- [x] Hold independent DB work through Close and release it explicitly before joining.
- [x] Repeat both plugin-stop variants12times and run neighboring lifecycle controls with race3times.
- [x] Reject omitted and premature DB-release mutants with the exact ordering assertions.
- [ ] Run exact-head CI; original Linux400ms timeout cause remains unproven.
