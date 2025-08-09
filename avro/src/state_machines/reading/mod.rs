use oval::Buffer;
use serde::Deserialize;

use crate::{
    Error, Schema,
    error::Details,
    state_machines::reading::{
        block::BlockStateMachine,
        bytes::BytesStateMachine,
        commands::{CommandTape, UnionVariants},
        object::ObjectStateMachine,
    },
    types::Value,
};

pub mod async_impl;
pub mod block;
pub mod bytes;
pub mod codec;
mod commands;
pub mod object;
mod object_container_file;
pub mod sync;

pub trait StateMachine: Sized {
    type Output: Sized;

    /// Start/continue the state machine.
    ///
    /// Implementers are not allowed to return until they can't make progress anymore.
    fn parse(self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output>;
}

/// Indicates whether the state machine has completed or needs to be polled again.
#[must_use]
pub enum StateMachineControlFlow<StateMachine, Output> {
    /// The state machine needs more data before it can continue.
    NeedMore(StateMachine),
    /// The state machine is done and the result is returned.s
    Done(Output),
}

pub type StateMachineResult<StateMachine, Output> =
    Result<StateMachineControlFlow<StateMachine, Output>, Error>;

/// The sub state machine that is currently being driven.
///
/// The `Int`, `Long`, `Float`, `Double`, and `Enum` statemachines don't have state, as
/// they don't consume the buffer if there are not enough bytes. This means that the only
/// thing these statemachines are keeping track of is which type we're actually decoding.
#[derive(Default)]
pub enum SubStateMachine {
    // TODO: Remove None, replace with Option<Box<SubStateMachine>>
    #[default]
    None,
    Int,
    Long,
    Float,
    Double,
    Enum,
    Bytes(BytesStateMachine),
    String(BytesStateMachine),
    Fixed(BytesStateMachine),
    Block(BlockStateMachine),
    Object(ObjectStateMachine),
    Union(UnionVariants),
}

/// A item that was read from the document.
#[must_use]
pub enum ItemRead {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Box<[u8]>),
    String(Box<str>),
    // TODO: Maybe this can just be a bytes
    Fixed(Box<[u8]>),
    /// The variant of the Enum that was read.
    Enum(u32),
    /// The variant of the Union that was read.
    ///
    /// The variant data is next.
    Union(u32),
    /// The start of a block of a Map or Array.
    Block(usize),
}

/// Read a zigzagged varint from the buffer.
///
/// Will only consume the buffer if a whole number has been read.
/// If insufficient bytes are available it will return `Ok(None)` to
/// indicate it needs more bytes.
pub fn decode_zigzag_buffer(buffer: &mut Buffer) -> Result<Option<i64>, Error> {
    if let Some((decoded, consumed)) = decode_zigzag_slice(buffer.data())? {
        buffer.consume(consumed);
        Ok(Some(decoded))
    } else {
        Ok(None)
    }
}

pub fn decode_zigzag_slice(buffer: &[u8]) -> Result<Option<(i64, usize)>, Error> {
    let mut decoded = 0;
    let mut loops_done = 0;
    let mut last_byte = 0;

    for (counter, &byte) in buffer.iter().take(9).enumerate() {
        let byte = u64::from(byte);
        decoded |= (byte & 0x7F) << (counter * 7);
        loops_done = counter;
        last_byte = byte;
        if byte >> 7 == 0 {
            break;
        }
    }

    if last_byte >> 7 != 0 {
        if loops_done == 9 {
            Err(Details::IntegerOverflow.into())
        } else {
            Ok(None)
        }
    } else {
        if decoded & 0x1 == 0 {
            Ok(Some(((decoded >> 1) as i64, loops_done)))
        } else {
            Ok(Some((!(decoded >> 1) as i64, loops_done)))
        }
    }
}

/// Moves `src` into the referenced `dest`, dropping the previous `dest` value.
pub fn replace_drop<T>(dest: &mut T, src: T) {
    let _ = std::mem::replace(dest, src);
}

/// Deserialize a tape to a [`Value`] using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
///
/// The tape is completely drained in the process.
pub fn value_from_tape(tape: &mut Vec<ItemRead>, _schema: &Schema) -> Result<Value, Error> {
    tape.clear();
    todo!();
}

/// Deserialize a tape to `T` using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
///
/// The tape is completely drained in the process.
pub fn deserialize_from_tape<'a, T: Deserialize<'a>>(
    tape: &mut Vec<ItemRead>,
    _schema: &Schema,
) -> Result<T, Error> {
    tape.clear();
    todo!()
}
