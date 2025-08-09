use miniz_oxide::inflate::core::DecompressorOxide;

use crate::{state_machines::reading::StateMachine, Codec};



pub struct CodecStateMachine<T: StateMachine> {
    sub_machine: T,
    codec: Codec,
}

pub enum Decoder {
    Null,
    Deflate(DecompressorOxide),
    #[cfg(feature = "snappy")]
    Snappy(snap::raw::Decoder)
}
