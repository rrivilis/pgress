//! Opcode classification for the session manager dataplane.
//!
//! The session manager never parses record bodies — it classifies opcodes from
//! the `u16` discriminant in `IsaHeader` alone. Each class has a distinct
//! admission treatment (see `admission.rs`).

/// Coarse classification of an ISA opcode for session manager routing.
///
/// The session manager uses this to decide admission treatment, routing path
/// (control vs. data), and backpressure behaviour — all without touching the
/// record body.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OpcodeClass {
    /// Structure-mutating ops — NodeCreate, EdgeConnect, DelNode, DelEdge,
    /// PartitionCreate, PartitionBind, SetPartitionAuthority, SetEdgeLabel,
    /// SetStabilizationConfig, SetExecutionPolicy, SetMode.
    /// Routed to control pipeline; serialized; subject to control-plane quota.
    Control,
    /// SetValue (0x0003). Coalesced under pressure (last writer wins per node uid).
    SetValue,
    /// Propagate (0x0004). Sheddable when a newer SetValue for the same root is queued.
    Propagate,
    /// Subscribe (0x0005). Treated as a lightweight control op.
    Subscribe,
    /// Demand (0x0008). Prioritised; MUST NOT be shed while caller is blocked.
    Demand,
    /// Stabilize (0x000B). Slow-lane budget; preemptable via WorkCursor.
    Stabilize,
    /// SessionProfile (0x0100) — first standard extension opcode.
    /// Routed to control pipeline for capability verification before any data ops.
    Profile,
    /// Known data ops not individually classified above (Reflect 0x000A).
    Data,
    /// Unrecognised opcode. Session manager skips body via `length` field.
    Unknown,
}

impl OpcodeClass {
    /// Classify a raw opcode value from `IsaHeader.opcode`.
    pub fn from_opcode(opcode: u16) -> Self {
        match opcode {
            // NodeCreate, EdgeConnect
            0x0001 | 0x0002 => Self::Control,
            0x0003           => Self::SetValue,
            0x0004           => Self::Propagate,
            0x0005           => Self::Subscribe,
            // DelNode, DelEdge
            0x0006 | 0x0007 => Self::Control,
            0x0008           => Self::Demand,
            // SetMode
            0x0009           => Self::Control,
            // Reflect
            0x000A           => Self::Data,
            0x000B           => Self::Stabilize,
            // PartitionCreate, PartitionBind, SetPartitionAuthority, SetEdgeLabel,
            // SetStabilizationConfig, SetExecutionPolicy
            0x000C ..= 0x0011 => Self::Control,
            // RegionDeclare — compilation hint; serialized through control pipeline
            0x0012           => Self::Control,
            // SessionProfile — first standard extension
            0x0100           => Self::Profile,
            _                => Self::Unknown,
        }
    }

    /// True for ops that must be serialized through the control pipeline.
    pub fn is_control_plane(self) -> bool {
        matches!(self, Self::Control | Self::Profile | Self::Subscribe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_value_classified() {
        assert_eq!(OpcodeClass::from_opcode(0x0003), OpcodeClass::SetValue);
    }

    #[test]
    fn demand_classified() {
        assert_eq!(OpcodeClass::from_opcode(0x0008), OpcodeClass::Demand);
    }

    #[test]
    fn stabilize_classified() {
        assert_eq!(OpcodeClass::from_opcode(0x000B), OpcodeClass::Stabilize);
    }

    #[test]
    fn partition_create_is_control() {
        assert!(OpcodeClass::from_opcode(0x000C).is_control_plane());
    }

    #[test]
    fn session_profile_is_control() {
        assert!(OpcodeClass::from_opcode(0x0100).is_control_plane());
    }

    #[test]
    fn unknown_opcode() {
        assert_eq!(OpcodeClass::from_opcode(0x8000), OpcodeClass::Unknown);
        assert_eq!(OpcodeClass::from_opcode(0xFFFF), OpcodeClass::Unknown);
    }
}
