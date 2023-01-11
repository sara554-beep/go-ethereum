package triedb

// Config defines all necessary options for database.
type Config struct {
	Cache     int    // Memory allowance (MB) to use for caching trie nodes in memory
	Journal   string // Journal of clean cache to survive node restarts
	Preimages bool   // Flag whether the preimage of trie key is recorded

	// Configs for experimental path-based scheme, not used yet.
	Scheme       string // Disk scheme for reading/writing trie nodes, hash-based as default
	StateHistory uint64 // Number of recent blocks to maintain state history for
	DirtySize    int    // Maximum memory allowance (MB) for caching dirty nodes
	ReadOnly     bool   // Flag whether the database is opened in read only mode.
}
