package auth

import (
	"context"
	"slices"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// FineGrainedTagChange selects one documented native user tag operation.
type FineGrainedTagChange uint8

const (
	ReplaceFineGrainedTags FineGrainedTagChange = iota + 1
	AddFineGrainedTags
	RemoveFineGrainedTags
)

// ChangeUserTags writes all selected native users in one system transaction.
// Tags are security metadata and use the same publication order as roles.
func (runtime *FineGrainedRuntime) ChangeUserTags(ctx context.Context, names []string, change FineGrainedTagChange, tags []string, ifExists bool) error {
	directory := runtime.directory.Users
	if directory == nil || change < ReplaceFineGrainedTags || change > RemoveFineGrainedTags {
		return ErrFineGrainedForbidden
	}
	tags = slices.Clone(tags)
	slices.Sort(tags)
	tags = slices.Compact(tags)
	return directory.writeUserRecords(ctx, func() (userRecordChange, error) {
		staged := make(map[string]*User, len(names))
		for _, name := range names {
			current, exists := directory.storedUser(name)
			if !exists {
				if ifExists {
					continue
				}
				return userRecordChange{}, ErrUserNotFound
			}
			user := cloneStoredUser(current)
			switch change {
			case ReplaceFineGrainedTags:
				user.SecurityTags = tags
			case AddFineGrainedTags:
				user.SecurityTags = append(slices.Clone(user.SecurityTags), tags...)
				slices.Sort(user.SecurityTags)
				user.SecurityTags = slices.Compact(user.SecurityTags)
			case RemoveFineGrainedTags:
				user.SecurityTags = slices.DeleteFunc(slices.Clone(user.SecurityTags), func(tag string) bool {
					return slices.Contains(tags, tag)
				})
			}
			user.UpdatedAt = time.Now()
			staged[name] = user
		}
		return userRecordChange{
			persist: func(ctx context.Context) error {
				return WithStoreTransaction(ctx, directory.storage, func(view storage.Engine) error {
					for _, user := range staged {
						if err := directory.saveUser(ctx, user, view); err != nil {
							return err
						}
					}
					return nil
				})
			},
			publish: func() {
				for name, user := range staged {
					directory.users[name] = user
					directory.logAudit(AuditEvent{EventType: "user_tags_change", Username: name, UserID: user.ID, Success: true})
				}
			},
		}, nil
	})
}

// UserTags returns the native directory's security metadata for an
// authorized SHOW USERS projection. Disabled users still have tags.
func (runtime *FineGrainedRuntime) UserTags(username string) []string {
	directory := runtime.directory.Users
	if directory == nil {
		return nil
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	if user := directory.users[username]; user != nil {
		return slices.Clone(user.SecurityTags)
	}
	return nil
}
