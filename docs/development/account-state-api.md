# Account state Go API in v1.4.4

Use `Authenticator.ChangeUserState` with an explicit `auth.UserStateChange`:

```go
err := authenticator.ChangeUserState("alice", auth.UserEnable)
err = authenticator.ChangeUserState("alice", auth.UserDisable)
err = authenticator.ChangeUserState("alice", auth.UserUnlock)
```

These replace `EnableUser`, `DisableUser` and `UnlockUser`; update Go callers
rather than retaining forwarding methods. HTTP, Bolt and administration syntax
retain their existing behavior. The zero action and unknown actions are errors.

| Action | Disabled | FailedLogins | LockedUntil |
| --- | --- | --- | --- |
| UserEnable | false | 0 | zero |
| UserDisable | true | preserved | preserved |
| UserUnlock | preserved | 0 | zero |

State, role, profile and password edits clone the cached user under the existing
authenticator lock, apply the requested transition and persist it before updating
the live cache or emitting a success audit event. Rejected persistence leaves the
live state unchanged. A successful password change invokes registered password
listeners after persistence and the success audit, still under that lock.
Listeners must not reenter an operation that takes the same lock.

This consolidation changes no Badger keys, stored user fields, payload encoding,
WAL schema or storage version. It preserves the existing integer normalization
of stored failed-login counters and the original failure audit for an incorrect
old password. Unknown users retain `ErrUserNotFound` for valid actions.
