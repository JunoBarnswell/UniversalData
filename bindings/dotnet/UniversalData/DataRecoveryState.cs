using System.Text.Json;

namespace UniversalData;

public enum DataRecoveryState { NoDecision, Committed, RolledBack, RecoveryRequired }
