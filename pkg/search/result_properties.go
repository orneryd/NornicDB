package search

type resultPropertyProjection struct {
	groupBy string
	include map[string]struct{}
	exclude map[string]struct{}
}

func newResultPropertyProjection(include, exclude []string) resultPropertyProjection {
	projection := resultPropertyProjection{}
	if len(include) > 0 {
		projection.include = make(map[string]struct{}, len(include))
		for _, property := range include {
			if property != "" {
				projection.include[property] = struct{}{}
			}
		}
	}
	if len(exclude) > 0 {
		projection.exclude = make(map[string]struct{}, len(exclude))
		for _, property := range exclude {
			if property != "" {
				projection.exclude[property] = struct{}{}
			}
		}
	}
	return projection
}

func (p resultPropertyProjection) apply(properties map[string]any) map[string]any {
	if len(p.include) == 0 && len(p.exclude) == 0 {
		return properties
	}
	capacity := len(properties)
	if len(p.include) > 0 && len(p.include) < capacity {
		capacity = len(p.include)
	}
	result := make(map[string]any, capacity)
	for property, value := range properties {
		if len(p.include) > 0 {
			if _, included := p.include[property]; !included {
				continue
			}
		}
		if _, excluded := p.exclude[property]; excluded {
			continue
		}
		result[property] = value
	}
	return result
}
