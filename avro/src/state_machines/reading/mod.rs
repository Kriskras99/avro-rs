use crate::{
    Decimal, Duration, Error, Schema,
    bigdecimal::deserialize_big_decimal,
    error::Details,
    schema::{
        ArraySchema, EnumSchema, FixedSchema, MapSchema, Name, NamesRef, Namespace, RecordSchema,
        ResolvedSchema, UnionSchema,
    },
    state_machines::reading::{
        block::BlockStateMachine, bytes::BytesStateMachine, commands::CommandTape,
        error::ValueFromTapeError, object::ObjectStateMachine, union::UnionStateMachine,
    },
    types::Value,
};
use oval::Buffer;
use serde::Deserialize;
use std::{borrow::Borrow, collections::HashMap, io::Read, ops::Deref, str::FromStr};
use uuid::Uuid;

pub mod async_impl;
pub mod block;
pub mod bytes;
pub mod codec;
mod commands;
pub mod error;
pub mod object;
mod object_container_file;
pub mod sync;
mod union;

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
pub enum SubStateMachine {
    Null(Vec<ItemRead>),
    Bool(Vec<ItemRead>),
    Int(Vec<ItemRead>),
    Long(Vec<ItemRead>),
    Float(Vec<ItemRead>),
    Double(Vec<ItemRead>),
    Enum(Vec<ItemRead>),
    Bytes {
        fsm: BytesStateMachine,
        read: Vec<ItemRead>,
    },
    String {
        fsm: BytesStateMachine,
        read: Vec<ItemRead>,
    },
    Block(BlockStateMachine),
    Object(ObjectStateMachine),
    Union(UnionStateMachine),
}

impl StateMachine for SubStateMachine {
    type Output = Vec<ItemRead>;

    fn parse(self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        match self {
            SubStateMachine::Null(mut read) => {
                read.push(ItemRead::Null);
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Bool(mut read) => {
                let mut byte = [0; 1];
                buffer
                    .read_exact(&mut byte)
                    .expect("Unreachable! Buffer is not empty");
                match byte {
                    [0] => read.push(ItemRead::Boolean(false)),
                    [1] => read.push(ItemRead::Boolean(true)),
                    [byte] => return Err(Details::BoolValue(byte).into()),
                }
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Int(mut read) => {
                let Some(n) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Int(read)));
                };
                let n = i32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                read.push(ItemRead::Int(n));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Long(mut read) => {
                let Some(n) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Long(read)));
                };
                read.push(ItemRead::Long(n));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Float(mut read) => {
                let Some(bytes) = buffer.data().first_chunk().copied() else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Float(read)));
                };
                buffer.consume(4);
                read.push(ItemRead::Float(f32::from_le_bytes(bytes)));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Double(mut read) => {
                let Some(bytes) = buffer.data().first_chunk().copied() else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Double(read)));
                };
                buffer.consume(8);
                read.push(ItemRead::Double(f64::from_le_bytes(bytes)));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Enum(mut read) => {
                let Some(n) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(StateMachineControlFlow::NeedMore(Self::Enum(read)));
                };
                // TODO: Wrong error
                let n = u32::try_from(n).map_err(|e| Details::ZagI32(e, n))?;
                read.push(ItemRead::Enum(n));
                Ok(StateMachineControlFlow::Done(read))
            }
            SubStateMachine::Bytes { fsm, mut read } => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::Bytes { fsm, read }))
                }
                StateMachineControlFlow::Done(bytes) => {
                    read.push(ItemRead::Bytes(bytes));
                    Ok(StateMachineControlFlow::Done(read))
                }
            },
            SubStateMachine::String { fsm, mut read } => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::String {
                        fsm,
                        read,
                    }))
                }
                StateMachineControlFlow::Done(bytes) => {
                    let string = String::from_utf8(bytes).map_err(Details::ConvertToUtf8)?;
                    read.push(ItemRead::String(string));
                    Ok(StateMachineControlFlow::Done(read))
                }
            },
            SubStateMachine::Block(fsm) => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::Block(fsm)))
                }
                StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            },
            SubStateMachine::Union(fsm) => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::Union(fsm)))
                }
                StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            },
            SubStateMachine::Object(fsm) => match fsm.parse(buffer)? {
                StateMachineControlFlow::NeedMore(fsm) => {
                    Ok(StateMachineControlFlow::NeedMore(Self::Object(fsm)))
                }
                StateMachineControlFlow::Done(read) => Ok(StateMachineControlFlow::Done(read)),
            },
        }
    }
}

/// A item that was read from the document.
#[derive(Debug)]
#[must_use]
pub enum ItemRead {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    // TODO: smollvec/hipbytes?
    Bytes(Vec<u8>),
    // TODO: smollstr/hipstr?
    String(String),
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

    for (counter, &byte) in buffer.iter().take(10).enumerate() {
        let byte = u64::from(byte);
        decoded |= (byte & 0x7F) << (counter * 7);
        loops_done = counter;
        last_byte = byte;
        if byte >> 7 == 0 {
            break;
        }
    }

    if last_byte >> 7 != 0 {
        if loops_done == 10 {
            Err(Details::IntegerOverflow.into())
        } else {
            Ok(None)
        }
    } else if decoded & 0x1 == 0 {
        Ok(Some(((decoded >> 1) as i64, loops_done + 1)))
    } else {
        Ok(Some((!(decoded >> 1) as i64, loops_done + 1)))
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
/// # Panics
/// Can panic if the provided schema does not exactly match the schema used to create the tape. To
/// convert between the writer and reader schema use [`Value::resolve`] instead.
pub fn value_from_tape(
    tape: &mut Vec<ItemRead>,
    schema: &Schema,
    names: &NamesRef,
) -> Result<Value, Error> {
    value_from_tape_internal(&mut tape.drain(..), schema, names, &None)
}

/// Recursivily transform the `tape` into a [`Value`] according to the provided [`Schema`].
pub fn value_from_tape_internal(
    tape: &mut impl Iterator<Item = ItemRead>,
    schema: &Schema,
    names: &NamesRef,
    enclosing_namespace: &Namespace,
) -> Result<Value, Error> {
    match schema {
        Schema::Null => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Null => Ok(Value::Null),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Boolean => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Boolean(bool) => Ok(Value::Boolean(bool)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Int => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Int(bool) => Ok(Value::Int(bool)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Long => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Long(long) => Ok(Value::Long(long)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Float => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Float(float) => Ok(Value::Float(float)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Double => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Double(double) => Ok(Value::Double(double)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Bytes => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Bytes(bytes) => Ok(Value::Bytes(bytes)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::String => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::String(string) => Ok(Value::String(string)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Array(ArraySchema { items, .. }) => {
            let mut collected = Vec::new();
            loop {
                let n = match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                    ItemRead::Block(n) => Ok(n),
                    item => Err(ValueFromTapeError::TapeSchemaMismatch {
                        schema: schema.clone(),
                        item,
                    }),
                }?;
                if n == 0 {
                    break;
                }
                collected.reserve(n);
                for _ in 0..n {
                    collected.push(value_from_tape_internal(
                        tape,
                        items,
                        names,
                        enclosing_namespace,
                    )?);
                }
            }
            Ok(Value::Array(collected))
        }
        Schema::Map(MapSchema { types, .. }) => {
            let mut collected = HashMap::new();
            loop {
                let n = match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                    ItemRead::Block(n) => Ok(n),
                    item => Err(ValueFromTapeError::TapeSchemaMismatch {
                        schema: schema.clone(),
                        item,
                    }),
                }?;
                if n == 0 {
                    break;
                }
                collected.reserve(n);
                for _ in 0..n {
                    let key = match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                        ItemRead::String(string) => Ok(string),
                        item => Err(ValueFromTapeError::TapeSchemaMismatch {
                            schema: Schema::String,
                            item,
                        }),
                    }?;
                    let val = value_from_tape_internal(tape, types, names, enclosing_namespace)?;
                    collected.insert(key, val);
                }
            }
            Ok(Value::Map(collected))
        }
        Schema::Union(UnionSchema { schemas, .. }) => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Union(variant) => {
                    let schema = schemas.get(usize::try_from(variant).unwrap()).ok_or(
                        Details::GetUnionVariant {
                            index: variant as i64,
                            num_variants: schemas.len(),
                        },
                    )?;
                    let value = Box::new(value_from_tape_internal(
                        tape,
                        schema,
                        names,
                        enclosing_namespace,
                    )?);
                    Ok(Value::Union(variant, value))
                }
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::Record(RecordSchema { name, fields, .. }) => {
            let fqn = name.fully_qualified_name(enclosing_namespace);
            let mut collected = Vec::with_capacity(fields.len());
            for field in fields {
                let collect = value_from_tape_internal(tape, &field.schema, names, &fqn.namespace)?;
                collected.push((field.name.clone(), collect));
            }
            Ok(Value::Record(collected))
        }
        Schema::Enum(EnumSchema { symbols, .. }) => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Enum(val) => Ok(Value::Enum(
                    val,
                    symbols.get(usize::try_from(val).unwrap()).unwrap().clone(),
                )),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::Fixed(FixedSchema { size, .. }) => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Bytes(fixed) => {
                    if *size == fixed.len() {
                        Ok(Value::Fixed(fixed.len(), fixed))
                    } else {
                        Err(ValueFromTapeError::TapeSchemaMismatchFixed {
                            expected: *size,
                            actual: fixed.len(),
                        }
                        .into())
                    }
                }
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::Decimal(_) => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Bytes(bytes) => Ok(Value::Decimal(Decimal::from(&bytes))),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::BigDecimal => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Bytes(bytes) => deserialize_big_decimal(&bytes).map(Value::BigDecimal),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Uuid => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::String(string) => Uuid::from_str(&string)
                .map(Value::Uuid)
                .map_err(|e| Details::ConvertStrToUuid(e).into()),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Date => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Int(int) => Ok(Value::Date(int)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::TimeMillis => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Int(int) => Ok(Value::TimeMillis(int)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::TimeMicros => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Long(long) => Ok(Value::TimeMicros(long)),
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::TimestampMillis => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::TimestampMillis(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::TimestampMicros => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::TimestampMicros(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::TimestampNanos => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::TimestampNanos(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::LocalTimestampMillis => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::LocalTimestampMillis(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::LocalTimestampMicros => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::LocalTimestampMicros(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::LocalTimestampNanos => {
            match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
                ItemRead::Long(long) => Ok(Value::LocalTimestampNanos(long)),
                item => Err(ValueFromTapeError::TapeSchemaMismatch {
                    schema: schema.clone(),
                    item,
                }
                .into()),
            }
        }
        Schema::Duration => match tape.next().ok_or(ValueFromTapeError::UnexpectedEndOfTape)? {
            ItemRead::Bytes(bytes) => {
                let array: [u8; 12] = bytes.deref().try_into().unwrap();
                Ok(Value::Duration(Duration::from(array)))
            }
            item => Err(ValueFromTapeError::TapeSchemaMismatch {
                schema: schema.clone(),
                item,
            }
            .into()),
        },
        Schema::Ref { name } => {
            let fqn = name.fully_qualified_name(enclosing_namespace);
            if let Some(resolved) = names.get(&fqn) {
                value_from_tape_internal(tape, resolved.borrow(), names, &fqn.namespace)
            } else {
                Err(Details::SchemaResolutionError(fqn).into())
            }
        }
    }
}

/// Deserialize a tape to `T` using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
pub fn deserialize_from_tape<'a, T: Deserialize<'a>>(
    tape: &mut Vec<ItemRead>,
    schema: &Schema,
) -> Result<T, Error> {
    let rs = ResolvedSchema::try_from(schema)?;
    deserialize_from_tape_internal(tape, schema, rs.get_names(), &None)
}

/// Recursively transform the `tape` into a `T` according to the provided [`Schema`].
fn deserialize_from_tape_internal<'a, T: Deserialize<'a>, S: Borrow<Schema>>(
    tape: &mut Vec<ItemRead>,
    _schema: &Schema,
    _names: &HashMap<Name, S>,
    _enclosing_namespace: &Namespace,
) -> Result<T, Error> {
    tape.clear();
    todo!()
}
