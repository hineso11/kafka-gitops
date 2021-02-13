package internal

type ReconcileActionType string

type ReconcileAction struct {
	Type ReconcileActionType
	Subject string
}

const (
	CreateActionType = ReconcileActionType("CREATE")
	UpdateActionType = ReconcileActionType("UPDATE")
	NoopActionType = ReconcileActionType("NOOP")
)
