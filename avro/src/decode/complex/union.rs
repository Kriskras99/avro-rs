//! State machine for the union type.

use crate::{
    Schema,
    decode::{SubFsm, decode_zigzag_buffer},
    error::Details,
    schema::{NamesRef, UnionSchema},
    types::Value,
    util::low_level::{Fsm, FsmControlFlow, FsmResult},
};
use oval::Buffer;

/// Decode a union.
///
/// A union is encoded by first writing an int value indicating the zero-based position within the
/// union of the schema of its value. The value is then encoded per the indicated schema within the
/// union.
pub struct UnionFsm<'a> {
    schemas: &'a [Schema],
    names: &'a NamesRef<'a>,
    index: Option<u32>,
    /// The state machine for decoding the variant.
    ///
    /// This is only set after the index has been decoded, and the variant state machine could not
    /// be completed immediately with the bytes available in the buffer.
    variant_fsm: Option<Box<SubFsm<'a>>>,
}

impl<'a> UnionFsm<'a> {
    pub fn new(schema: &'a UnionSchema, names: &'a NamesRef<'a>) -> UnionFsm<'a> {
        Self {
            schemas: schema.variants(),
            names,
            index: None,
            variant_fsm: None,
        }
    }
}

impl<'a> Fsm for UnionFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // We need the index to know which schema to use for decoding the value
        // and because we need to return it in `Value::Union`
        let index = match self.index {
            None => {
                // We haven't read the index yet, so try reading it
                let Some(n) = decode_zigzag_buffer(buffer)? else {
                    return Ok(FsmControlFlow::NeedMore(self));
                };
                // Make sure the index is valid
                if let Ok(index) = u32::try_from(n)
                    && index
                        < u32::try_from(self.schemas.len())
                            .expect("Union must not have more than u32 variants")
                {
                    self.index = Some(index);
                    index
                } else {
                    return Err(Details::GetUnionVariant {
                        index: n,
                        num_variants: self.schemas.len(),
                    }
                    .into());
                }
            }
            Some(index) => index,
        };
        // Start or continue decoding the variant
        match self.variant_fsm.as_deref_mut() {
            None => {
                let schema = &self.schemas[index as usize];

                match SubFsm::new(schema, self.names)?.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        self.variant_fsm = Some(Box::new(fsm));
                        Ok(FsmControlFlow::NeedMore(self))
                    }
                    FsmControlFlow::Done(value) => Ok(FsmControlFlow::Done(Value::Union(
                        index as u32,
                        Box::new(value),
                    ))),
                }
            }
            Some(variant_fsm) => {
                let fsm = std::mem::take(variant_fsm);
                match fsm.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        let _ = std::mem::replace(variant_fsm, fsm);
                        Ok(FsmControlFlow::NeedMore(self))
                    }
                    FsmControlFlow::Done(value) => Ok(FsmControlFlow::Done(Value::Union(
                        index as u32,
                        Box::new(value),
                    ))),
                }
            }
        }
    }
}
