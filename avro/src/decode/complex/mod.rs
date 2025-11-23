use crate::{
    decode::decode_zigzag_buffer,
    error::Details,
    schema::EnumSchema,
    types::Value,
    util::low_level::{Fsm, FsmControlFlow, FsmResult},
};
use oval::Buffer;

pub mod block;
pub mod record;
pub mod union;

pub struct EnumFsm<'a> {
    schema: &'a EnumSchema,
}
impl<'a> EnumFsm<'a> {
    pub fn new(schema: &'a EnumSchema) -> Self {
        Self { schema }
    }
}
impl<'a> Fsm for EnumFsm<'a> {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        let Some(n) = decode_zigzag_buffer(buffer)? else {
            // Not enough data left in the buffer
            return Ok(FsmControlFlow::NeedMore(self));
        };
        let n = u32::try_from(n).map_err(|_| Details::GetEnumUnknownIndexValue)?;
        // If we truncate the value with `as usize` instead of try_from we might get a valid index
        // value.
        let n_as_usize = usize::try_from(n).map_err(|_| Details::GetEnumUnknownIndexValue)?;
        let symbol = self
            .schema
            .symbols
            .get(n_as_usize)
            .cloned()
            .ok_or(Details::GetEnumUnknownIndexValue)?;
        Ok(FsmControlFlow::Done(Value::Enum(n, symbol)))
    }
}
