/// The state machines used in [`Reader`] and [`AsyncReader`] to decode Avro datums.
/// 
/// These should not be used directly unless you are implementing your own reader.

use crate::{
    error::Details, util::decode_variable, Error
};
use block::BlockStateMachine;
use bytes::BytesStateMachine;
use commands::CommandTape;
use datum::DatumStateMachine;
use items::ItemRead;
use union::UnionStateMachine;
use oval::Buffer;
use std::{io::Read};

pub mod async_impl;
pub mod block;
pub mod bytes;
pub mod codec;
mod commands;
pub mod datum;
pub mod error;
mod object_container_file;
pub mod sync;
mod union;
pub mod items;

/// A state machine that can progress as long as there are new bytes to consume.
/// 
/// # Usage
/// The state machine expects to run through a byte stream from start to end. It however does
/// not expect to have all bytes available to it at once. It will keep returning [`StateMachineControlFlow::NeedMore`]
/// while it's expecting more bytes, if the byte stream is finished but the state machine isn't this
/// indicates a corrupt byte stream.
/// 
// /// ```no_run
// /// let mut fsm = SubStateMachine::Bool(Vec::new());
// /// let mut buffer = Buffer::with_capacity(2048);
// /// let result = loop {
// ///     let n = byte_stream.read(buffer.space())?;
// ///     buffer.fill(n);
// /// 
// ///     match fsm.parse(&mut buffer)? {
//             StateMachineControlFlow::NeedMore(new_fsm) => fsm = new_fsm,
//             StateMachineControlFlow::Done(result) => break result,
// ///     }
// /// }
// /// ```
pub trait StateMachine: Sized {
    /// The output type of the state machine.
    type Output: Sized;

    /// Start/continue the state machine.
    ///
    /// Implementers are not allowed to return until they can't make progress.
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

/// The state machine that is currently being driven.
/// 
/// The basic statemachines only track the current type being decoded and have the item tape.
/// The more complex statemachines have their own implementation of [`StateMachine`] that's
/// being delegated to.
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
    Object(DatumStateMachine),
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

#[cfg(test)]
#[allow(clippy::expect_fun_call)]
mod tests {
    use crate::{
        Decimal,
        encode::{encode, tests::success},
        from_avro_datum,
        schema::{DecimalSchema, FixedSchema, Schema},
        types::{
            Value,
            Value::{Array, Int, Map},
        },
    };
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn test_decode_array_without_size() -> TestResult {
        let mut input: &[u8] = &[6, 2, 4, 6, 0];

        let result = from_avro_datum(&Schema::array(Schema::Int), &mut input, None)?;

        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result);

        Ok(())
    }

    #[test]
    fn test_decode_array_with_size() -> TestResult {
        let mut input: &[u8] = &[5, 6, 2, 4, 6, 0];
        let result = from_avro_datum(&Schema::array(Schema::Int), &mut input, None)?;
        assert_eq!(Array(vec!(Int(1), Int(2), Int(3))), result);

        Ok(())
    }

    #[test]
    fn test_decode_map_without_size() -> TestResult {
        let mut input: &[u8] = &[0x02, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = from_avro_datum(&Schema::map(Schema::Int), &mut input, None)?;
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result);

        Ok(())
    }

    #[test]
    fn test_decode_map_with_size() -> TestResult {
        let mut input: &[u8] = &[0x01, 0x0C, 0x08, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00];
        let result = from_avro_datum(&Schema::map(Schema::Int), &mut input, None)?;
        let mut expected = HashMap::new();
        expected.insert(String::from("test"), Int(1));
        assert_eq!(Map(expected), result);

        Ok(())
    }

    #[test]
    fn test_negative_decimal_value() -> TestResult {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let inner = Box::new(Schema::Fixed(
            FixedSchema::builder()
                .name(Name::new("decimal")?)
                .size(2)
                .build(),
        ));
        let schema = Schema::Decimal(DecimalSchema {
            inner,
            precision: 4,
            scale: 2,
        });
        let bigint = (-423).to_bigint().unwrap();
        let value = Value::Decimal(Decimal::from(bigint.to_signed_bytes_be()));

        let mut buffer = Vec::new();
        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));

        let mut bytes = &buffer[..];
        let result = from_avro_datum(&schema, &mut bytes, None)?;
        assert_eq!(result, value);

        Ok(())
    }

    #[test]
    fn test_decode_decimal_with_bigger_than_necessary_size() -> TestResult {
        use crate::{encode::encode, schema::Name};
        use num_bigint::ToBigInt;
        let inner = Box::new(Schema::Fixed(FixedSchema {
            size: 13,
            name: Name::new("decimal")?,
            aliases: None,
            doc: None,
            default: None,
            attributes: Default::default(),
        }));
        let schema = Schema::Decimal(DecimalSchema {
            inner,
            precision: 4,
            scale: 2,
        });
        let value = Value::Decimal(Decimal::from(
            ((-423).to_bigint().unwrap()).to_signed_bytes_be(),
        ));
        let mut buffer = Vec::<u8>::new();

        encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));
        let mut bytes: &[u8] = &buffer[..];
        let result = from_avro_datum(&schema, &mut bytes, None)?;
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
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value1,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
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
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value2,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
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
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
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
        let mut bytes = &buf[..];
        assert_eq!(
            outer_value,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
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
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_1,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
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
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_1,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_2, &schema, &mut buf)
            .expect(&success(&outer_record_variation_2, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_2,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
                "Failed to Decode with recursively defined namespace with schema:\n {:?}\n",
                &schema
            ))
        );

        let mut buf = Vec::new();
        encode(&outer_record_variation_3, &schema, &mut buf)
            .expect(&success(&outer_record_variation_3, &schema));
        let mut bytes = &buf[..];
        assert_eq!(
            outer_record_variation_3,
            from_avro_datum(&schema, &mut bytes, None).expect(&format!(
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

        let result = from_avro_datum(&Schema::Uuid, &mut &buffer[..], None)?;
        assert_eq!(result, value);

        Ok(())
    }

    // TODO: Schema::Uuid needs a sub schema which is either String or Fixed. It's now part of the
    //       spec anyway.
    // #[test]
    // fn avro_3926_encode_decode_uuid_to_fixed() -> TestResult {
    //     use crate::encode::encode;
    //
    //     let schema = Schema::Fixed(FixedSchema {
    //         size: 16,
    //         name: "uuid".into(),
    //         aliases: None,
    //         doc: None,
    //         default: None,
    //         attributes: Default::default(),
    //     });
    //     let value = Value::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?);
    //
    //     let mut buffer = Vec::new();
    //     encode(&value, &schema, &mut buffer).expect(&success(&value, &schema));
    //
    //     let result = from_avro_datum(&Schema::Uuid, &mut &buffer[..], None)?;
    //     assert_eq!(result, value);
    //
    //     Ok(())
    // }
}
