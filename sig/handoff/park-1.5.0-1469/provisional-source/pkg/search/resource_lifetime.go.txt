package search

import (
 "context"
 "github.com/orneryd/nornicdb/pkg/lifecycle"
)

// ResourceOwner lets public adapters use the same DB admission before resolving
// a request's database or starting a continuation, including cache/ID paths.
func(s *Service) ResourceOwner()*lifecycle.Owner{return s.lifetime}
// StopBackgroundWork cancels and joins every relationship producer before the
// DB releases providers. Public readers drain through that same DB owner.
func(s *Service) StopBackgroundWork(){if s.workers!=nil{_=s.workers.Stop(context.Background(),nil)}}
