/// Items decoded from the byte stream.

use std::{borrow::Borrow, collections::HashMap, ops::Deref as _, str::FromStr as _};

use serde::Deserialize;
use uuid::Uuid;

use crate::{bigdecimal::deserialize_big_decimal, error::Details, schema::{ArraySchema, EnumSchema, FixedSchema, MapSchema, Name, Names, Namespace, RecordSchema, ResolvedSchema, UnionSchema}, decode::error::ValueFromTapeError, types::Value, Decimal, Duration, Error, Schema};

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


/// Deserialize a tape to a [`Value`] using the provided [`Schema`].
///
/// The schema must be compatible with the schema used by the original writer.
///
/// Both `names` and `extra_names` are checked when a [`Schema::Ref`] is encountered. They're allowed
/// to have overlapping items.
///
/// # Panics
/// Can panic if the provided schema does not exactly match the schema used to create the tape. To
/// convert between the writer and reader schema use [`Value::resolve`] instead.
pub fn value_from_tape(
    tape: &mut Vec<ItemRead>,
    schema: &Schema,
    names: &Names,
) -> Result<Value, Error> {
    value_from_tape_internal(&mut tape.drain(..), schema, &None, names)
}

/// Recursively transform the `tape` into a [`Value`] according to the provided [`Schema`].
///
/// Both `names` and `extra_names` are checked when a [`Schema::Ref`] is encountered. They're allowed
/// to have overlapping items.
fn value_from_tape_internal(
    tape: &mut impl Iterator<Item = ItemRead>,
    schema: &Schema,
    enclosing_namespace: &Namespace,
    names: &Names,
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
                        enclosing_namespace,
                        names,
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
                    let val = value_from_tape_internal(tape, types, enclosing_namespace, names)?;
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
                        enclosing_namespace,
                        names,
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
                let collect = value_from_tape_internal(tape, &field.schema, &fqn.namespace, names)?;
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
            ItemRead::Bytes(bytes) => {
                Uuid::from_slice(&bytes)
                .map(Value::Uuid)
                .map_err(|e| Details::UuidFromSlice(e).into())
            }
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
                value_from_tape_internal(tape, resolved, &fqn.namespace, names)
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
