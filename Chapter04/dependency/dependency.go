package dependency

import "golang.org/x/xerrors"

// DepType describes a dependency type.
type DepType int

// The supported dependency types returned by the API.
const (
	DepTypeProject DepType = iota
	DepTypeResource
)

// API is implemented by clients of the project metadata micro-service.
type API interface {
	// ListDependencies returns the list of direct dependency IDs for a
	// particular project ID or an error if a non-project ID argument is
	// provided.
	ListDependencies(projectID string) ([]string, error)

	// DependencyType returns the type of a particular dependency.
	DependencyType(dependencyID string) (DepType, error)
}

// Collector provides helper methods for building the dependency graph of projects.
type Collector struct {
	api API
}

// NewCollector returns a new dependency collector instance that uses api.
func NewCollector(api API) *Collector {
	return &Collector{api: api}
}

// AllDependencies returns the unique list of direct and transitive dependecies
// for the project with the specified ID.
func (c *Collector) AllDependencies(projectID string) ([]string, error) {
	ctx := newDepContext(projectID)
	for ctx.HasUncheckedDeps() {
		projectID = ctx.NextUncheckedDep()
		projectDeps, err := c.api.ListDependencies(projectID)
		if err != nil {
			return nil, xerrors.Errorf("unable to list dependencies for project %q: %w", projectID, err)
		}
		if err = c.scanProjectDependencies(ctx, projectDeps); err != nil {
			return nil, err
		}
	}
	return ctx.depList, nil
}

// scanProjectDependencies adds each item in depList to the list of direct and
// transitive dependencies maintained by ctx. In addition, it checks the type
// of each dependency in depList and enqueues any encountered sub-projects for
// recursive scanning.
func (c *Collector) scanProjectDependencies(ctx *depCtx, depList []string) error {
	for _, depID := range depList {
		if ctx.AlreadyChecked(depID) {
			continue
		}
		ctx.AddToDepList(depID)
		depType, err := c.api.DependencyType(depID)
		if err != nil {
			return xerrors.Errorf("unable to get dependency type for id %q: %w", depID, err)
		}
		if depType == DepTypeProject {
			ctx.AddToUncheckedList(depID)
		}
	}
	return nil
}

type depCtx struct {
	depList   []string
	unchecked []string
	checked   map[string]struct{}
}

func newDepContext(projectID string) *depCtx {
	return &depCtx{
		unchecked: []string{projectID},
		checked:   make(map[string]struct{}),
	}
}

func (ctx *depCtx) HasUncheckedDeps() bool {
	return len(ctx.unchecked) != 0
}

func (ctx *depCtx) NextUncheckedDep() string {
	if len(ctx.unchecked) == 0 {
		return ""
	}

	next := ctx.unchecked[0]
	ctx.unchecked = ctx.unchecked[1:]
	return next
}

func (ctx *depCtx) AlreadyChecked(id string) bool {
	_, checked := ctx.checked[id]
	return checked
}

func (ctx *depCtx) AddToDepList(id string) {
	ctx.depList = append(ctx.depList, id)
	ctx.checked[id] = struct{}{}
}

func (ctx *depCtx) AddToUncheckedList(id string) {
	ctx.unchecked = append(ctx.unchecked, id)
}
