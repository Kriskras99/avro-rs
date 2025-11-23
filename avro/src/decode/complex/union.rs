use crate::{
    decode::{SubFsm, decode_zigzag_buffer},
    error::Details,
    schema::{NamesRef, UnionSchema},
    types::Value,
    util::low_level::{Fsm, FsmControlFlow, FsmResult},
};
use oval::Buffer;

pub struct UnionFsm<'a> {
    schema: &'a UnionSchema,
    names: &'a NamesRef<'a>,
    index: Option<u32>,
    sub_fsm: Option<Box<SubFsm<'a>>>,
}

impl<'a> UnionFsm<'a> {
    pub fn new(schema: &'a UnionSchema, names: &'a NamesRef<'a>) -> UnionFsm<'a> {
        Self {
            schema,
            names,
            index: None,
            sub_fsm: None,
        }
    }
}

impl<'a> Fsm for UnionFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        if self.index.is_none() {
            let Some(n) = decode_zigzag_buffer(buffer)? else {
                return Ok(FsmControlFlow::NeedMore(self));
            };
            self.index = Some(u32::try_from(n).map_err(|_| Details::GetUnionVariant {
                index: n,
                num_variants: self.schema.schemas.len(),
            })?);
        }
        let index = self.index.unwrap_or_else(|| unreachable!());
        if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
            let fsm = std::mem::take(sub_fsm);
            match fsm.parse(buffer)? {
                FsmControlFlow::NeedMore(fsm) => {
                    let _ = std::mem::replace(sub_fsm, fsm);
                    Ok(FsmControlFlow::NeedMore(self))
                }
                FsmControlFlow::Done(value) => {
                    Ok(FsmControlFlow::Done(Value::Union(index, Box::new(value))))
                }
            }
        } else {
            let index_usize = usize::try_from(index).map_err(|_| Details::GetUnionVariant {
                index: i64::from(index),
                num_variants: self.schema.schemas.len(),
            })?;
            let schema = self
                .schema
                .schemas
                .get(index_usize)
                .ok_or(Details::EmptyUnion)?;

            match SubFsm::new(schema, self.names)?.parse(buffer)? {
                FsmControlFlow::NeedMore(fsm) => {
                    self.sub_fsm = Some(Box::new(fsm));
                    Ok(FsmControlFlow::NeedMore(self))
                }
                FsmControlFlow::Done(value) => {
                    Ok(FsmControlFlow::Done(Value::Union(index, Box::new(value))))
                }
            }
        }
    }
}
