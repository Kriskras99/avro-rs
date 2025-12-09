//! State machine for the record type

use crate::{
    Schema,
    decode::SubFsm,
    schema::{NamesRef, RecordSchema, SchemaKind},
    types::Value,
    util::low_level::{Fsm, FsmControlFlow, FsmResult},
};
use oval::Buffer;

/// Decode a record.
///
/// A record is encoded by encoding the values of its fields in the order that they are declared.
/// In other words, a record is encoded as just the concatenation of the encodings of its fields.
/// Field values are encoded per their schema.
pub struct RecordFsm<'a> {
    schema: &'a RecordSchema,
    names: &'a NamesRef<'a>,
    /// The index of the field in `RecordSchema.fields` that is currently being decoded.
    current_field: usize,
    /// The state machine for decoding the current field.
    ///
    /// This will only be set if not all fields could be decoded immediately from the buffer.
    field_fsm: Option<Box<SubFsm<'a>>>,
    /// Already decoded fields.
    fields: Vec<(String, Value)>,
}
impl<'a> RecordFsm<'a> {
    pub fn new(schema: &'a RecordSchema, names: &'a NamesRef<'a>) -> Self {
        Self {
            schema,
            names,
            current_field: 0,
            field_fsm: None,
            fields: Vec::with_capacity(schema.fields.len()),
        }
    }
}

impl<'a> Fsm for RecordFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // All fields are there, this only possible for an empty record
        if self.current_field >= self.schema.fields.len() {
            return Ok(FsmControlFlow::Done(Value::Record(self.fields)));
        }
        loop {
            if let Some(field_fsm) = self.field_fsm.as_deref_mut() {
                let fsm = std::mem::take(field_fsm);
                match fsm.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        let _ = std::mem::replace(field_fsm, fsm);
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        // Finished reading a field, add the name and value to the list
                        let field_name = self.schema.fields[self.current_field].name.clone();
                        self.fields.push((field_name, value));
                        assert_eq!(self.current_field, self.fields.len() - 1);

                        self.current_field += 1;

                        // If there is a next field, prepare the state machine in the same box
                        if let Some(field) = self.schema.fields.get(self.current_field) {
                            let _ = std::mem::replace(
                                field_fsm,
                                SubFsm::new(&field.schema, self.names)?,
                            );
                            // Restart the loop
                            continue;
                        } else {
                            return Ok(FsmControlFlow::Done(Value::Record(self.fields)));
                        }
                    }
                }
            } else {
                let schema = &self.schema.fields[self.current_field].schema;
                match SubFsm::new(schema, self.names)?.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        self.field_fsm = Some(Box::new(fsm));
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        // Finished reading a field, add the name and value to the list
                        let field_name = self.schema.fields[self.current_field].name.clone();
                        self.fields.push((field_name, value));
                        assert_eq!(self.current_field, self.fields.len() - 1);

                        self.current_field += 1;

                        // If there is no next field, return the record
                        if self.schema.fields.get(self.current_field).is_none() {
                            assert_eq!(self.fields.len(), self.schema.fields.len());
                            return Ok(FsmControlFlow::Done(Value::Record(self.fields)));
                        }
                    }
                }
            }
        }
    }
}
