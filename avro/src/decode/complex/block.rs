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
/// Arrays are encoded as a series of blocks. Each block consists of a long count value, followed by
/// that many array items. A block with count zero indicates the end of the array. Each item is
/// encoded per the array’s item schema.
///
/// If a block’s count is negative, its absolute value is used, and the count is followed immediately
/// by a long block size indicating the number of bytes in the block.
pub struct ArrayFsm<'a> {
    schema: &'a Schema,
    names: &'a NamesRef<'a>,
    /// The state machine for decoding the current item.
    ///
    /// This will only be set if any item state machine could not be completed immediately because
    /// there were not enough bytes in the buffer.
    item_fsm: Option<Box<SubFsm<'a>>>,
    /// The amount of items left in the current block.
    ///
    /// If this is zero, the block count value needs to be read.
    left_in_current_block: usize,
    /// Indicates of the block byte size needs to be read.
    ///
    /// This is only set if the block count value was negative and will be reset after reading the
    /// block size.
    need_to_read_block_byte_size: bool,
    /// The items that were already decoded.
    items: Vec<Value>,
}
impl<'a> ArrayFsm<'a> {
    pub fn new(schema: &'a Schema, names: &'a NamesRef<'a>) -> Self {
        Self {
            schema,
            names,
            item_fsm: None,
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
            items: Vec::new(),
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
                    return Ok(FsmControlFlow::Done(Value::Array(self.items)));
                }

                // Need to read the block byte size when block is negative
                self.need_to_read_block_byte_size = block.is_negative();

                // We do the rest with the absolute block size
                let abs_block = usize::try_from(block.unsigned_abs())
                    .map_err(|e| Details::ConvertU64ToUsize(e, block.unsigned_abs()))?;

                self.left_in_current_block = abs_block;
                // Reserve additional space for the new items
                self.items.reserve(abs_block);
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
            if let Some(sub_fsm) = self.item_fsm.as_deref_mut() {
                let fsm = std::mem::take(sub_fsm);
                match fsm.parse(buffer)? {
                    FsmControlFlow::NeedMore(fsm) => {
                        let _ = std::mem::replace(sub_fsm, fsm);
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        self.left_in_current_block -= 1;
                        self.items.push(value);
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
                        self.item_fsm = Some(Box::new(fsm));
                        return Ok(FsmControlFlow::NeedMore(self));
                    }
                    FsmControlFlow::Done(value) => {
                        self.left_in_current_block -= 1;
                        self.items.push(value);
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
/// Maps are encoded as a series of blocks. Each block consists of a long count value, followed by
/// that many key/value pairs. A block with count zero indicates the end of the map. Each item is
/// encoded per the map’s value schema.
//
/// If a block’s count is negative, its absolute value is used, and the count is followed immediately
/// by a long block size indicating the number of bytes in the block.
pub struct MapFsm<'a> {
    schema: &'a Schema,
    names: &'a NamesRef<'a>,
    /// The state machine for the current key or value.
    ///
    /// This will only be set if any key or value state machine could not be completed immediately
    /// because there were not enough bytes in the buffer.
    entry_fsm: Option<Box<SubFsm<'a>>>,
    /// The amount of entries left in the current block.
    ///
    /// If this is zero, the block count value needs to be read.
    left_in_current_block: usize,
    /// Indicates of the block byte size needs to be read.
    ///
    /// This is only set if the block count value was negative and will be reset after reading the
    /// block size.
    need_to_read_block_byte_size: bool,
    /// The key for the value that is being read.
    ///
    /// If this is `None`, then the key is being decoded. Otherwise, the value is being decoded.
    current_key: Option<String>,
    /// The already collected entries.
    entries: HashMap<String, Value>,
}

impl<'a> MapFsm<'a> {
    pub fn new(schema: &'a Schema, names: &'a NamesRef<'a>) -> Self {
        Self {
            schema,
            names,
            entry_fsm: None,
            left_in_current_block: 0,
            need_to_read_block_byte_size: false,
            current_key: None,
            entries: HashMap::new(),
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
                    return Ok(FsmControlFlow::Done(Value::Map(self.entries)));
                }

                // Need to read the block byte size when block is negative
                self.need_to_read_block_byte_size = block.is_negative();

                // We do the rest with the absolute block size
                let abs_block = usize::try_from(block.unsigned_abs())
                    .map_err(|e| Details::ConvertU64ToUsize(e, block.unsigned_abs()))?;

                self.left_in_current_block = abs_block;
                // Reserve additional space for all new entries
                self.entries.reserve(abs_block);
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
                if let Some(sub_fsm) = self.entry_fsm.as_deref_mut() {
                    let fsm = std::mem::take(sub_fsm);
                    match fsm.parse(buffer)? {
                        FsmControlFlow::NeedMore(fsm) => {
                            let _ = std::mem::replace(sub_fsm, fsm);
                            self.current_key = Some(key);
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            self.left_in_current_block -= 1;
                            // TODO: Are duplicate entries allowed?
                            self.entries.insert(key, value);
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
                            self.entry_fsm = Some(Box::new(fsm));
                            self.current_key = Some(key);
                            return Ok(FsmControlFlow::NeedMore(self));
                        }
                        FsmControlFlow::Done(value) => {
                            self.left_in_current_block -= 1;
                            self.entries.insert(key, value);
                            // As we don't have a box yet, we don't create the next state machine as that
                            // allocation might be unnecessary
                            continue;
                        }
                    }
                }
            } else {
                // Check if we already have a state machine to run
                if let Some(sub_fsm) = self.entry_fsm.as_deref_mut() {
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
                            self.entry_fsm = Some(Box::new(fsm));
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
