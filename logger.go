package cachier

// Logger is interface for logging
type Logger interface {
	Error(...interface{})
	Warn(...interface{})
	Print(...interface{})
}

// DummyLogger is implementation of Logger that does not log anything
type DummyLogger struct{}

// Error does nothing
func (d DummyLogger) Error(...interface{}) {}

// Warn does nothing
func (d DummyLogger) Warn(...interface{}) {}

// Print does nothing
func (d DummyLogger) Print(...interface{}) {}
