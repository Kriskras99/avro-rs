use crate::{
    Error,
    decode::{SubFsm, codec::CodecStateMachine, decode_zigzag_buffer, object_container::Header},
    error::Details,
    types::Value,
    util::low_level::{Fsm, FsmControlFlow, FsmResult},
};
use oval::Buffer;
use std::io::Read;

#[ouroboros::self_referencing]
pub struct DataBlockFsm<'a> {
    /// The header of the file being read.
    ///
    /// Can be created using [`HeaderFsm`].
    ///
    /// [`HeaderFsm`]: super::header::HeaderFsm
    header: Header<'a>,
    /// The state machine for decompressing the data and reading datum from it.
    ///
    /// Because we can only access this self-referential field through a (mutable) reference but the
    /// [`Fsm`] trait takes `self`, it is wrapped in an [`Option`] which is *always* filled except
    /// during the `Fsm::parse` function.
    #[borrows(header)]
    #[covariant]
    fsm: Option<CodecStateMachine<SubFsm<'this>>>,
    /// Before we read the block size, we need to read the sync marker.
    need_to_read_sync: bool,
    /// How many items are left in the current block.
    left_in_block: usize,
    /// After we read the block size, we also need to read the byte size.
    need_to_read_block_byte_size: bool,
    /// Are we at a block border and is the buffer completely empty.
    ///
    /// This is used so that the caller can interpret [`FsmControlFlow::NeedMore`]
    /// as finished, if it knows the file is finished.
    buffer_empty_at_block_border: bool,
}

impl<'a> DataBlockFsm<'a> {
    pub fn simple_new(header: Header<'a>) -> Result<Self, Error> {
        Self::try_new(
            header,
            |header| -> Result<_, Error> {
                let inner = SubFsm::new(header.schema(), header.names())?;
                let codec = CodecStateMachine::new(inner, header.codec());
                Ok(Some(codec))
            },
            false,
            0,
            false,
            false,
        )
    }

    pub fn header(&self) -> &Header<'_> {
        self.borrow_header()
    }

    pub fn into_header(self) -> Header<'a> {
        self.into_heads().header
    }

    pub fn buffer_empty_at_block_border(&self) -> bool {
        *self.borrow_buffer_empty_at_block_border()
    }
}

impl<'a> Fsm for DataBlockFsm<'a> {
    type Output = (Value, DataBlockFsm<'a>);

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        // Because DataBlockFsm is a self-referential struct, we can't operate on it directly.
        // So we use `with_mut`, which gives us a view of DataBlockFsm with mutable references to
        // every field. Because this function takes a mutable reference to `self` we can't return
        // immediately but need to replace `()` with `self` at the end.
        let cf = self.with_mut(|this| -> FsmResult<(), Value> {
            // If we have just finished a block or have just been created we need to read the block metadata
            if *this.left_in_block == 0 {
                // At the end of a block we need to read the sync marker
                if *this.need_to_read_sync {
                    if buffer.available_data() < 16 {
                        return Ok(FsmControlFlow::NeedMore(()));
                    }

                    let mut sync = [0; 16];
                    buffer
                        .read_exact(&mut sync)
                        .unwrap_or_else(|_| unreachable!());

                    if sync != this.header.sync() {
                        return Err(Error::new(Details::GetBlockMarker));
                    }
                    *this.need_to_read_sync = false;
                }

                // Read the amount of items in the block
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    *this.buffer_empty_at_block_border = buffer.empty();
                    return Ok(FsmControlFlow::NeedMore(()));
                };

                let abs_block = block.unsigned_abs();
                let abs_block = usize::try_from(abs_block)
                    .map_err(|e| Details::ConvertU64ToUsize(e, abs_block))?;
                *this.need_to_read_block_byte_size = true;
                // This will only be done after left_in_block hits 0
                *this.need_to_read_sync = true;
                *this.left_in_block = abs_block;
            }

            if *this.need_to_read_block_byte_size {
                let Some(block) = decode_zigzag_buffer(buffer)? else {
                    // Not enough data left in the buffer
                    return Ok(FsmControlFlow::NeedMore(()));
                };
                // We can't use the size, but we should check that it is valid
                let _size =
                    usize::try_from(block).map_err(|e| Details::ConvertI64ToUsize(e, block))?;
                *this.need_to_read_block_byte_size = false;
            }

            match this.fsm.take().unwrap().parse(buffer)? {
                FsmControlFlow::NeedMore(fsm) => {
                    let _ = this.fsm.insert(fsm);
                    Ok(FsmControlFlow::NeedMore(()))
                }
                FsmControlFlow::Done((value, fsm)) => {
                    *this.left_in_block -= 1;

                    // Codec's inner FSM is finished so needs to be reset
                    this.fsm
                        .insert(fsm)
                        .reset(SubFsm::new(this.header.schema(), this.header.names())?);

                    Ok(FsmControlFlow::Done(value))
                }
            }
        })?;

        // We can't use FsmControlFlow::map as Rust can't tell only one of the closures is actually
        // called and therefore thinks we try to apply `self` to both.
        match cf {
            FsmControlFlow::NeedMore(()) => Ok(FsmControlFlow::NeedMore(self)),
            FsmControlFlow::Done(value) => Ok(FsmControlFlow::Done((value, self))),
        }
    }
}
