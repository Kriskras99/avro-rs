//! State machines for block-based complex types.
//!
//! # Block state machines
//!
//! Both arrays and map use the same underlying block-based representation, where a map is just an
//! array of tuples of a string and the underlying type. Although the implementations of [`ArrayFsm`]
//! and [`MapFsm`] are very similar, they do not share a common implementation to allow for some
//! optimizations in the map machinery.

use crate::{
    Schema,
    decode::{SubFsm, decode_zigzag_buffer, primitive::bytes::StringFsm},
    error::Details,
    schema::NamesRef,
    types::Value,
    util::{
        low_level::{Fsm, FsmControlFlow, FsmResult},
        safe_len,
    },
};
use oval::Buffer;
use std::collections::HashMap;

/// Decode an array.
///
/// An array is composed of many blocks, where every block can store many items. A block starts with
/// the item count encoded as a varint. If the item count of a block is 0, the end of the array has
/// been reached. If the item count is negative, the item count is followed by the block size in
/// bytes (excluding the header) encoded as a varint. This header is followed by the encoded items.
pub struct ArrayFsm<'a> {
    schema: &'a Schema,
    names: &'a NamesRef<'a>,
    sub_fsm: Option<Box<SubFsm<'a>>>,
    left_in_current_block: usize,
    need_to_read_block_byte_size: bool,
    values: Vec<Value>,
}
impl<'a> ArrayFsm<'a> {
    pub fn new(schema: &'a Schema, names: &'a NamesRef<'a>) -> Self {
        Self {
            schema,
            names,
            sub_fsm: None,
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
            values: Vec::new(),
        }
    }
}
impl<'a> Fsm for ArrayFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // We loop until we're finished or we need more data
        loop {
            // If we finished the last block (or are newly created) read the block info
            if self.left_in_current_block == 0 {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    return Ok(FsmControlFlow::NeedMore(self));
                };

                // Done parsing the blocks
                if block == 0 {
                    return Ok(FsmControlFlow::Done(Value::Array(self.values)));
                }

                // Need to read the block byte size when block is negative
                self.need_to_read_block_byte_size = block.is_negative();

                // We do the rest with the absolute block size
                let abs_block = usize::try_from(block.unsigned_abs())
                    .map_err(|e| Details::ConvertU64ToUsize(e, block.unsigned_abs()))?;

                self.left_in_current_block = abs_block;
                self.values.reserve(abs_block);
            }

            // If the block length was negative we need to read the block size
            if self.need_to_read_block_byte_size {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    return Ok(FsmControlFlow::NeedMore(self));
                };

                // Make sure the value is sane
                let _ = safe_len(
                    usize::try_from(block).map_err(|e| Details::ConvertI64ToUsize(e, block))?,
                )?;

                self.need_to_read_block_byte_size = false;
            }

            // Check if we already have a state machine to run
            if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
                let fsm = std::mem::take(sub_fsm);
                match fsm.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        let _ = std::mem::replace(sub_fsm, fsm);
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        self.left_in_current_block -= 1;
                        self.values.push(value);
                        // Assume we need to read another value
                        // We do this to reuse the Box we already have and therefore preventing a lot
                        // of allocations
                        let _ = std::mem::replace(sub_fsm, SubFsm::new(self.schema, self.names)?);
                        // Continue the loop
                        continue;
                    }
                }
            } else {
                let fsm = SubFsm::new(self.schema, self.names)?;
                match fsm.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        // Save the current progress and ask for more bytes
                        self.sub_fsm = Some(Box::new(fsm));
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        self.left_in_current_block -= 1;
                        self.values.push(value);
                        // As we don't have a box yet, we don't create the next state machine as that
                        // allocation might be unnecessary
                        continue;
                    }
                }
            }
        }
    }
}

/// Decode a map.
///
/// A map is composed of many blocks, where every block can store many entries. A block starts with
/// the entry count encoded as a varint. If the entry count of a block is 0, the end of the map has
/// been reached. If the entry count is negative, the entry count is followed by the block size in
/// bytes (excluding the header) encoded as a varint. This header is followed by the encoded entry.
/// Every entry is encoded with the key (always a string) followed by the value.
pub struct MapFsm<'a> {
    schema: &'a Schema,
    names: &'a NamesRef<'a>,
    sub_fsm: Option<Box<SubFsm<'a>>>,
    left_in_current_block: usize,
    need_to_read_block_byte_size: bool,
    current_key: Option<String>,
    values: HashMap<String, Value>,
}
impl<'a> MapFsm<'a> {
    pub fn new(schema: &'a Schema, names: &'a NamesRef<'a>) -> Self {
        Self {
            schema,
            names,
            sub_fsm: None,
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
            current_key: None,
            values: HashMap::new(),
        }
    }
}
impl<'a> Fsm for MapFsm<'a> {
    type Output = Value;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // We loop until we're finished or we need more data
        loop {
            // If we finished the last block (or are newly created) read the block info
            if self.left_in_current_block == 0 {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    return Ok(FsmControlFlow::NeedMore(self));
                };

                // Done parsing the blocks
                if block == 0 {
                    return Ok(FsmControlFlow::Done(Value::Map(self.values)));
                }

                // Need to read the block byte size when block is negative
                self.need_to_read_block_byte_size = block.is_negative();

                // We do the rest with the absolute block size
                let abs_block = usize::try_from(block.unsigned_abs())
                    .map_err(|e| Details::ConvertU64ToUsize(e, block.unsigned_abs()))?;

                self.left_in_current_block = abs_block;
                self.values.reserve(abs_block);
            }

            // If the block length was negative we need to read the block size
            if self.need_to_read_block_byte_size {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    return Ok(FsmControlFlow::NeedMore(self));
                };

                // Make sure the value is sane
                let _ = safe_len(
                    usize::try_from(block).map_err(|e| Details::ConvertI64ToUsize(e, block))?,
                )?;

                self.need_to_read_block_byte_size = false;
            }

            if let Some(key) = self.current_key.take() {
                // Check if we already have a state machine to run
                if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
                    let fsm = std::mem::take(sub_fsm);
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            let _ = std::mem::replace(sub_fsm, fsm);
                            self.current_key = Some(key);
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            self.left_in_current_block -= 1;
                            self.values.insert(key, value);
                            // Assume we need to read another key
                            // We do this to reuse the Box we already have and therefore preventing a lot
                            // of allocations
                            let _ =
                                std::mem::replace(sub_fsm, SubFsm::String(StringFsm::default()));
                            // Continue the loop
                            continue;
                        }
                    }
                } else {
                    let fsm = SubFsm::new(self.schema, self.names)?;
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            // Save the current progress and ask for more bytes
                            self.sub_fsm = Some(Box::new(fsm));
                            self.current_key = Some(key);
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            self.left_in_current_block -= 1;
                            self.values.insert(key, value);
                            // As we don't have a box yet, we don't create the next state machine as that
                            // allocation might be unnecessary
                            continue;
                        }
                    }
                }
            } else {
                // Check if we already have a state machine to run
                if let Some(sub_fsm) = self.sub_fsm.as_deref_mut() {
                    let fsm = std::mem::take(sub_fsm);
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            let _ = std::mem::replace(sub_fsm, fsm);
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            let Value::String(key) = value else {
                                unreachable!(
                                    "The key fsm is always StringFsm which can only return a String"
                                )
                            };
                            self.current_key = Some(key);
                            // Now we need to read the value, might as well reuse the allocation
                            let _ =
                                std::mem::replace(sub_fsm, SubFsm::new(self.schema, self.names)?);
                            // Continue the loop
                            continue;
                        }
                    }
                } else {
                    let fsm = SubFsm::String(StringFsm::default());
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            // Save the current progress and ask for more bytes
                            self.sub_fsm = Some(Box::new(fsm));
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            let Value::String(key) = value else {
                                unreachable!("StringFsm can only return a String")
                            };
                            self.current_key = Some(key);
                            // We don't have an allocation so there is nothing to reuse
                            continue;
                        }
                    }
                }
            }
        }
    }
}
