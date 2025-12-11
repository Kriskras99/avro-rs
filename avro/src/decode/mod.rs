// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Low-level decoders for binary Avro data.
//!
//! # Low-level decoders
//!
//! This module contains the low-level decoders for binary Avro data. It is strongly recommended to
//! use the high-level readers in [`reader`]. This should only be used if you are decoding a custom
//! file format or are using an async runtime not supported in [`reader`].
//!
//! # Usage
//! The decoder implementations are based on finite state machines, expressed through the [`Fsm`]
//! trait. A decoder is supplied with a [`Buffer`] that has some (but not necessarily all) data. The
//! decoder will decode as far as it can. If it does not have enough data, it will return
//! [`FsmControlFlow::NeedMore`]. By filling the buffer with more data, the decoder can be resumed.
//! If the end of the data stream is reached and the decoder is still returning [`FsmControlFlow::NeedMore`]
//! then the data stream is corrupt and an error should be thrown.
//!
//! If the decoder is finished decoding, it will return a [`FsmControlFlow::Done`] which will also
//! consume the decoder, preventing accidental reuse of an already finished decoder.
//!
//! The supplied buffer must have space for at least 16 bytes, any less and some decoders won't be
//! able to make progress.
//!
//! ```no_run
//! # use apache_avro::{Schema, decode2::{DatumFsm, Fsm, FsmControlFlow}};
//! # use oval::Buffer;
//! # use std::{fs::File, io::Read};
//! # let schema = Schema::Bytes;
//!
//! let mut buffer = Buffer::with_capacity(256);
//! let mut file = File::open("some").unwrap();
//! let mut fsm = DatumFsm::new(&schema);
//!
//! // If the schema is Schema::Null, it's perfectly valid for the file to be empty
//! // so we don't check the amount of bytes filled the first time.
//! let bytes_read = file.read(buffer.space()).unwrap();
//! buffer.fill(bytes_read);
//!
//! let result = loop {
//!     match fsm.parse(&mut buffer).unwrap() {
//!         FsmControlFlow::NeedMore(new_fsm) => {
//!             fsm = new_fsm;
//!             let bytes_read = file.read(buffer.space()).unwrap();
//!             if bytes_read == 0 {
//!                 panic!("File is finished but decoder is not");
//!             }
//!             buffer.fill(bytes_read);
//!         }
//!         FsmControlFlow::Done(value) => {
//!             break value;
//!         }
//!     }
//! };
//! println!("{result:?}");
//! ```
//!
//!
//! [`reader`]: crate::reader
//! [`FsmControlFlow::NeedMore`]: crate::util::low_level::FsmControlFlow::NeedMore
//! [`FsmControlFlow::Done`]: crate::util::low_level::FsmControlFlow::Done

mod codec;
mod complex;
mod logical;
pub mod object_container;
mod primitive;

use crate::{
    AvroResult, Error,
    decode::{
        complex::{
            EnumFsm,
            block::{ArrayFsm, MapFsm},
            record::RecordFsm,
            union::UnionFsm,
        },
        logical::{
            decimal::{BigDecimalFsm, DecimalFsm},
            time::{
                DateFsm, DurationFsm, LocalTimestampMicrosFsm, LocalTimestampMillisFsm,
                LocalTimestampNanosFsm, TimeMicrosFsm, TimeMillisFsm, TimestampMicrosFsm,
                TimestampMillisFsm, TimestampNanosFsm,
            },
            uuid::UuidFsm,
        },
        primitive::{
            BoolFsm, NullFsm,
            bytes::{BytesFsm, FixedFsm, StringFsm},
            floats::{DoubleFsm, FloatFsm},
            zigzag::{IntFsm, LongFsm},
        },
    },
    error::Details,
    schema::{ArraySchema, FixedSchema, MapSchema, NamesRef, Schema},
    types::Value,
    util::{
        low_level::{Fsm, FsmResult},
        safe_len,
    },
};
use oval::Buffer;
use std::io::ErrorKind;

/// Read a zigzagged varint from the buffer.
///
/// Will only consume the buffer if a whole number has been read.
/// If insufficient bytes are available it will return `Ok(None)` to
/// indicate it needs more bytes.
fn decode_zigzag_buffer(buffer: &mut Buffer) -> Result<Option<i64>, Error> {
    if let Some((decoded, consumed)) = decode_variable(buffer.data())? {
        buffer.consume(consumed);
        Ok(Some(decoded))
    } else {
        Ok(None)
    }
}

/// Decode a zigzag encoded length.
///
/// This version of [`decode_len`] will return a [`Details::ReadVariableIntegerBytes`] error if there are not
/// enough bytes and does not return the amount of bytes read.
///
/// See [`decode_len`] for more details.
pub(crate) fn decode_len_simple(buffer: &[u8]) -> AvroResult<(usize, usize)> {
    decode_len(buffer)?
        .ok_or_else(|| Details::ReadVariableIntegerBytes(ErrorKind::UnexpectedEof.into()).into())
}

/// Decode a zigzag encoded length.
///
/// This will use [`safe_len`] to check if the length is in allowed bounds.
///
/// # Returns
/// `Some(integer, bytes read)` if it completely read an integer, `None` if it did not have enough
/// bytes in the slice.
pub(crate) fn decode_len(buffer: &[u8]) -> AvroResult<Option<(usize, usize)>> {
    if let Some((integer, bytes)) = decode_variable(buffer)? {
        let length =
            usize::try_from(integer).map_err(|e| Details::ConvertI64ToUsize(e, integer))?;
        let safe = safe_len(length)?;
        Ok(Some((safe, bytes)))
    } else {
        Ok(None)
    }
}

/// Decode a zigzag encoded integer.
///
/// # Returns
/// `Some(integer, bytes read)` if it completely read an integer, `None` if it did not have enough
/// bytes in the slice.
pub(crate) fn decode_variable(buffer: &[u8]) -> Result<Option<(i64, usize)>, Error> {
    let mut decoded = 0;
    let mut loops_done = 0;
    let mut last_byte = 0;

    if buffer.is_empty() {
        return Ok(None);
    }

    for (counter, &byte) in buffer.iter().take(10).enumerate() {
        decoded |= u64::from(byte & 0x7F) << (counter * 7);
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
    } else if decoded & 0x1 == 0 {
        Ok(Some(((decoded >> 1) as i64, loops_done + 1)))
    } else {
        Ok(Some((!(decoded >> 1) as i64, loops_done + 1)))
    }
}

/// A state machine to parse an Avro datum.
pub struct DatumFsm<'a> {
    /// This is just a thin wrapper around [`SubFsm`] to hide the implementation details from the user.
    fsm: SubFsm<'a>,
}
impl<'a> DatumFsm<'a> {
    pub fn new(schema: &'a Schema, names: &'a NamesRef<'a>) -> Result<Self, Error> {
        Ok(Self {
            fsm: SubFsm::new(schema, names)?,
        })
    }
}
impl<'a> Fsm for DatumFsm<'a> {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        Ok(self.fsm.parse(buffer)?.map(|fsm| Self { fsm }, |v| v))
    }
}

enum SubFsm<'a> {
    Null(NullFsm),
    Boolean(BoolFsm),
    Int(IntFsm),
    Long(LongFsm),
    Float(FloatFsm),
    Double(DoubleFsm),
    Bytes(BytesFsm),
    String(StringFsm),
    Fixed(FixedFsm),
    Enum(EnumFsm<'a>),
    Union(UnionFsm<'a>),
    Array(ArrayFsm<'a>),
    Map(MapFsm<'a>),
    Record(RecordFsm<'a>),
    Date(DateFsm),
    Decimal(DecimalFsm),
    BigDecimal(BigDecimalFsm),
    TimeMillis(TimeMillisFsm),
    TimeMicros(TimeMicrosFsm),
    TimestampMillis(TimestampMillisFsm),
    TimestampMicros(TimestampMicrosFsm),
    TimestampNanos(TimestampNanosFsm),
    LocalTimestampMillis(LocalTimestampMillisFsm),
    LocalTimestampMicros(LocalTimestampMicrosFsm),
    LocalTimestampNanos(LocalTimestampNanosFsm),
    Duration(DurationFsm),
    Uuid(UuidFsm),
}

impl<'a> SubFsm<'a> {
    pub fn new(schema: &'a Schema, names: &'a NamesRef<'a>) -> Result<Self, Error> {
        match schema {
            Schema::Null => Ok(Self::Null(NullFsm)),
            Schema::Boolean => Ok(Self::Boolean(BoolFsm)),
            Schema::Int => Ok(Self::Int(IntFsm::default())),
            Schema::Long => Ok(Self::Long(LongFsm::default())),
            Schema::Float => Ok(Self::Float(FloatFsm)),
            Schema::Double => Ok(Self::Double(DoubleFsm)),
            Schema::Bytes => Ok(Self::Bytes(BytesFsm::default())),
            Schema::String => Ok(Self::String(StringFsm::default())),
            Schema::Array(ArraySchema { items, .. }) => {
                Ok(Self::Array(ArrayFsm::new(items, names)))
            }
            Schema::Map(MapSchema { types, .. }) => Ok(Self::Map(MapFsm::new(types, names))),
            Schema::Union(schema) => Ok(Self::Union(UnionFsm::new(schema, names))),
            Schema::Record(schema) => Ok(Self::Record(RecordFsm::new(schema, names))),
            Schema::Enum(schema) => Ok(Self::Enum(EnumFsm::new(schema))),
            Schema::Fixed(FixedSchema { size, .. }) => Ok(Self::Fixed(FixedFsm::new(*size))),
            Schema::Decimal(schema) => Ok(Self::Decimal(DecimalFsm::new(schema))),
            Schema::BigDecimal => Ok(Self::BigDecimal(BigDecimalFsm::default())),
            Schema::Uuid(schema) => Ok(Self::Uuid(UuidFsm::new(schema))),
            Schema::Date => Ok(Self::Date(DateFsm::default())),
            Schema::TimeMillis => Ok(Self::TimeMillis(TimeMillisFsm::default())),
            Schema::TimeMicros => Ok(Self::TimeMicros(TimeMicrosFsm::default())),
            Schema::TimestampMillis => Ok(Self::TimestampMillis(TimestampMillisFsm::default())),
            Schema::TimestampMicros => Ok(Self::TimestampMicros(TimestampMicrosFsm::default())),
            Schema::TimestampNanos => Ok(Self::TimestampNanos(TimestampNanosFsm::default())),
            Schema::LocalTimestampMillis => Ok(Self::LocalTimestampMillis(
                LocalTimestampMillisFsm::default(),
            )),
            Schema::LocalTimestampMicros => Ok(Self::LocalTimestampMicros(
                LocalTimestampMicrosFsm::default(),
            )),
            Schema::LocalTimestampNanos => {
                Ok(Self::LocalTimestampNanos(LocalTimestampNanosFsm::default()))
            }
            Schema::Duration => Ok(Self::Duration(DurationFsm::default())),
            Schema::Ref { name } => {
                if let Some(schema) = names.get(name) {
                    Self::new(schema, names)
                } else {
                    Err(Error::new(Details::SchemaResolutionError(name.clone())))
                }
            }
        }
    }
}

impl<'a> Default for SubFsm<'a> {
    fn default() -> Self {
        Self::Null(NullFsm)
    }
}

impl<'a> Fsm for SubFsm<'a> {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self {
            Self::Null(fsm) => Ok(fsm.parse(buffer)?.map(Self::Null, |v| v)),
            Self::Boolean(fsm) => Ok(fsm.parse(buffer)?.map(Self::Boolean, |v| v)),
            Self::Int(fsm) => Ok(fsm.parse(buffer)?.map(Self::Int, |v| v)),
            Self::Long(fsm) => Ok(fsm.parse(buffer)?.map(Self::Long, |v| v)),
            Self::Float(fsm) => Ok(fsm.parse(buffer)?.map(Self::Float, |v| v)),
            Self::Double(fsm) => Ok(fsm.parse(buffer)?.map(Self::Double, |v| v)),
            Self::Bytes(fsm) => Ok(fsm.parse(buffer)?.map(Self::Bytes, |v| v)),
            Self::String(fsm) => Ok(fsm.parse(buffer)?.map(Self::String, |v| v)),
            Self::Fixed(fsm) => Ok(fsm.parse(buffer)?.map(Self::Fixed, |v| v)),
            Self::Enum(fsm) => Ok(fsm.parse(buffer)?.map(Self::Enum, |v| v)),
            Self::Union(fsm) => Ok(fsm.parse(buffer)?.map(Self::Union, |v| v)),
            Self::Array(fsm) => Ok(fsm.parse(buffer)?.map(Self::Array, |v| v)),
            Self::Map(fsm) => Ok(fsm.parse(buffer)?.map(Self::Map, |v| v)),
            Self::Record(fsm) => Ok(fsm.parse(buffer)?.map(Self::Record, |v| v)),
            Self::Date(fsm) => Ok(fsm.parse(buffer)?.map(Self::Date, |v| v)),
            Self::Decimal(fsm) => Ok(fsm.parse(buffer)?.map(Self::Decimal, |v| v)),
            Self::BigDecimal(fsm) => Ok(fsm.parse(buffer)?.map(Self::BigDecimal, |v| v)),
            Self::TimeMillis(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimeMillis, |v| v)),
            Self::TimeMicros(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimeMicros, |v| v)),
            Self::TimestampMillis(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimestampMillis, |v| v)),
            Self::TimestampMicros(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimestampMicros, |v| v)),
            Self::TimestampNanos(fsm) => Ok(fsm.parse(buffer)?.map(Self::TimestampNanos, |v| v)),
            Self::LocalTimestampMillis(fsm) => {
                Ok(fsm.parse(buffer)?.map(Self::LocalTimestampMillis, |v| v))
            }
            Self::LocalTimestampMicros(fsm) => {
                Ok(fsm.parse(buffer)?.map(Self::LocalTimestampMicros, |v| v))
            }
            Self::LocalTimestampNanos(fsm) => {
                Ok(fsm.parse(buffer)?.map(Self::LocalTimestampNanos, |v| v))
            }
            Self::Duration(fsm) => Ok(fsm.parse(buffer)?.map(Self::Duration, |v| v)),
            Self::Uuid(fsm) => Ok(fsm.parse(buffer)?.map(Self::Uuid, |v| v)),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_fun_call)]
mod tests {
    use crate::{
        Decimal, Error,
        decode::{SubFsm, decode_variable},
        encode::{encode, tests::success},
        error::Details,
        schema::{
            DecimalSchema, FixedSchema, InnerDecimalSchema, ResolvedSchema, Schema, UuidSchema,
        },
        types::Value::{self, Array, Int, Map},
        util::low_level::{Fsm, FsmControlFlow},
    };
    use apache_avro_test_helper::TestResult;
    use oval::Buffer;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn decode(schema: &Schema, input: &[u8]) -> Result<Value, Error> {
        let resolved = ResolvedSchema::try_from(schema)?;
        let mut buffer = Buffer::from_slice(input);
        let fsm = SubFsm::new(schema, resolved.get_names())?;
        match fsm.parse(&mut buffer)? {
            FsmControlFlow::NeedMore(_) => {
                panic!("Should not be NeedMore");
            }
            FsmControlFlow::Done(value) => Ok(value),
        }
    }

    #[test]
    fn test_decode_variable_overflow() {
        let causes_left_shift_overflow: &[u8] = &[0xe1; 10];
        assert!(matches!(
            decode_variable(causes_left_shift_overflow)
                .unwrap_err()
                .details(),
            Details::IntegerOverflow
        ));
    }

    #[test]
    fn test_decode_array_without_size() -> TestResult {
        let input: &[u8] = &[6, 2, 4, 6, 0];
        let result = decode(&Schema::array(Schema::Int), input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result?);

        Ok(())
    }

    #[test]
    fn test_decode_array_with_size() -> TestResult {
        let input: &[u8] = &[5, 6, 2, 4, 6, 0];
        let result = decode(&Schema::array(Schema::Int), input);
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result?);

        Ok(())
    }

    #[test]
    fn test_decode_map_without_size() -> TestResult {
        let input: &[u8] = &[0x02, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::map(Schema::Int), input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result?);

        Ok(())
    }

    #[test]
    fn test_decode_map_with_size() -> TestResult {
        let input: &[u8] = &[0x01, 0x0C, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = decode(&Schema::map(Schema::Int), input);
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result?);

        Ok(())
    }

    #[test]
    fn test_negative_decimal_value() -> TestResult {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let schema = Schema::Decimal(DecimalSchema {
            inner: InnerDecimalSchema::Fixed(
                FixedSchema::builder()
                    .name(Name::new("decimal")?)
                    .size(2)
                    .build(),
            ),
            precision: 4,
            scale: 2,
        });
        let bigint = (-423).to_bigint().unwrap();
        let value = Value::Decimal(Decimal::from(bigint.to_signed_bytes_be()));

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let bytes = &buffer[..];
        let result = decode(&schema, bytes)?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn test_decode_decimal_with_bigger_than_necessary_size() -> TestResult {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let schema = Schema::Decimal(DecimalSchema {
            inner: InnerDecimalSchema::Fixed(FixedSchema {
                size: 13,
                name: Name::new("decimal")?,
                aliases: None,
                doc: None,
                default: None,
                attributes: Default::default(),
            }),
            precision: 4,
            scale: 2,
        });
        let value = Value::Decimal(Decimal::from(
            ((-423).to_bigint().unwrap()).to_signed_bytes_be(),
        ));
        let mut buffer = Vec::<u8>::new();

        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));
        let bytes: &[u8] = &buffer[..];
        let result = decode(&schema, bytes)?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_union() -> TestResult {
        // if encoding fails in this test check the corresponding test in encode
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":[ "null", {
                        "type":"record",
                        "name": "Inner",
                        "fields": [ {
                            "name":"z",
                            "type":"int"
                        }]
                    }]
                },
                {
                    "name":"b",
                    "type":"Inner"
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value1 = Value::Record(vec![
            ("a".into(), Value::Union(1, Box::new(inner_value1))),
            ("b".into(), inner_value2.clone()),
        ]);
        let mut buf = Vec::new();
        encode(&outer_value1, &schema, &mut buf).expect(&success(&outer_value1, &schema));
        assert!(!buf.is_empty());
        let bytes = &buf[..];
        assert_eq!(
            outer_value1,
            decode(&schema, bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        let outer_value2 = Value::Record(vec![
            ("a".into(), Value::Union(0, Box::new(Value::Null))),
            ("b".into(), inner_value2),
        ]);
        encode(&outer_value2, &schema, &mut buf).expect(&success(&outer_value2, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_value2,
            decode(&schema, bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
            ))
        );

        Ok(())
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_array() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"array",
                        "items": {
                            "type":"record",
                            "name": "Inner",
                            "fields": [ {
                                "name":"z",
                                "type":"int"
                            }]
                        }
                    }
                },
                {
                    "name":"b",
                    "type": "Inner"
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            ("a".into(), Value::Array(vec![inner_value1])),
            ("b".into(), inner_value2),
        ]);
        let mut buf = Vec::new();
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_value,
            decode(&schema, bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
            ))
        );

        Ok(())
    }

    #[test]
    fn test_avro_3448_recursive_definition_decode_map() -> TestResult {
        let schema = Schema::parse_str(
            r#"
        {
            "type":"record",
            "name":"TestStruct",
            "fields": [
                {
                    "name":"a",
                    "type":{
                        "type":"map",
                        "values": {
                            "type":"record",
                            "name": "Inner",
                            "fields": [ {
                                "name":"z",
                                "type":"int"
                            }]
                        }
                    }
                },
                {
                    "name":"b",
                    "type": "Inner"
                }
            ]
        }"#,
        )?;

        let inner_value1 = Value::Record(vec![("z".into(), Value::Int(3))]);
        let inner_value2 = Value::Record(vec![("z".into(), Value::Int(6))]);
        let outer_value = Value::Record(vec![
            (
                "a".into(),
                Value::Map(vec![("akey".into(), inner_value1)].into_iter().collect()),
            ),
            ("b".into(), inner_value2),
        ]);
        let mut buf = Vec::new();
        encode(&outer_value, &schema, &mut buf).expect(&success(&outer_value, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_value,
            decode(&schema, bytes).expect(&format!(
                "Failed to decode using recursive definitions with schema:\n {:?}\n",
                &schema
            ))
        );

        Ok(())
    }

    #[test]
    fn test_avro_3448_proper_multi_level_decoding_middle_namespace() -> TestResult {
        // if encoding fails in this test check the corresponding test in encode
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "middle_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_record_variation_1,
            decode(&schema, bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            decode(&schema, bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            decode(&schema, bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        Ok(())
    }

    #[test]
    fn test_avro_3448_proper_multi_level_decoding_inner_namespace() -> TestResult {
        // if encoding fails in this test check the corresponding test in encode
        let schema = r#"
        {
          "name": "record_name",
          "namespace": "space",
          "type": "record",
          "fields": [
            {
              "name": "outer_field_1",
              "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "middle_record_name",
                            "namespace":"middle_namespace",
                            "fields":[
                                {
                                    "name":"middle_field_1",
                                    "type":[
                                        "null",
                                        {
                                            "type":"record",
                                            "name":"inner_record_name",
                                            "namespace":"inner_namespace",
                                            "fields":[
                                                {
                                                    "name":"inner_field_1",
                                                    "type":"double"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
            },
            {
                "name": "outer_field_2",
                "type" : "inner_namespace.inner_record_name"
            }
          ]
        }
        "#;
        let schema = Schema::parse_str(schema)?;
        let inner_record = Value::Record(vec![("inner_field_1".into(), Value::Double(5.4))]);
        let middle_record_variation_1 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(0, Box::new(Value::Null)),
        )]);
        let middle_record_variation_2 = Value::Record(vec![(
            "middle_field_1".into(),
            Value::Union(1, Box::new(inner_record.clone())),
        )]);
        let outer_record_variation_1 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_2 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_1)),
            ),
            ("outer_field_2".into(), inner_record.clone()),
        ]);
        let outer_record_variation_3 = Value::Record(vec![
            (
                "outer_field_1".into(),
                Value::Union(1, Box::new(middle_record_variation_2)),
            ),
            ("outer_field_2".into(), inner_record),
        ]);

        let mut buf = Vec::new();
        encode(&outer_record_variation_1, &schema, &mut buf)
            .expect(&success(&outer_record_variation_1, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_record_variation_1,
            decode(&schema, bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            decode(&schema, bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            decode(&schema, bytes).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        Ok(())
    }

    #[test]
    fn avro_3926_encode_decode_uuid_to_string() -> TestResult {
        use crate::encode::encode;

        let schema = Schema::String;
        let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let result = decode(&Schema::Uuid(UuidSchema::String), &buffer[..])?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn avro_3926_encode_decode_uuid_to_fixed() -> TestResult {
        use crate::encode::encode;

        let fixed = FixedSchema {
            size: 16,
            name: "uuid".into(),
            aliases: None,
            doc: None,
            default: None,
            attributes: Default::default(),
        };

        let schema = Schema::Fixed(fixed.clone());
        let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let result = decode(&Schema::Uuid(UuidSchema::Fixed(fixed)), &buffer[..])?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn encode_decode_uuid_to_bytes() -> TestResult {
        use crate::encode::encode;

        let schema = Schema::Bytes;
        let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let result = decode(&Schema::Uuid(UuidSchema::Bytes), &buffer[..])?;
        assert_eq!(result, value);

        Ok(())
    }
}
