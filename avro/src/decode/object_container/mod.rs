use crate::{
    Codec, Schema,
    decode::object_container::{data::DataBlockFsm, header::HeaderFsm},
    schema::NamesRef,
    types::Value,
    util::low_level::{Fsm, FsmControlFlow, FsmResult},
};
use oval::Buffer;
use std::{collections::HashMap, marker::PhantomData};

/// Decoder for the Object Container File data blocks.
mod data;
/// Decoder for the Object Container File header.
mod header;

/// The header as read from an Object Container File.
pub struct Header<'a> {
    header: InnerHeader<'a>,
}

impl<'a> Header<'a> {
    fn new(header: InnerHeader<'a>) -> Self {
        Header { header }
    }

    /// The schema used to write the file.
    pub fn schema(&self) -> &Schema {
        self.header.borrow_schema()
    }

    pub fn names(&self) -> &NamesRef<'_> {
        self.header.borrow_names()
    }

    /// The compression used.
    pub fn codec(&self) -> Codec {
        *self.header.borrow_codec()
    }

    /// The sync marker used between blocks
    pub fn sync(&self) -> [u8; 16] {
        *self.header.borrow_sync()
    }

    /// User metadata in the header
    pub fn metadata(&self) -> &HashMap<String, Vec<u8>> {
        self.header.borrow_metadata()
    }
}

#[ouroboros::self_referencing]
pub struct InnerHeader<'a> {
    /// The schema used to write the file.
    pub schema: Schema,
    /// We need to store the schemata so `names` can borrow it.
    pub schemata: Vec<&'a Schema>,
    #[borrows(schema, schemata)]
    #[covariant]
    pub names: NamesRef<'this>,
    /// The compression used.
    pub codec: Codec,
    /// The sync marker used between blocks
    pub sync: [u8; 16],
    /// User metadata in the header
    pub metadata: HashMap<String, Vec<u8>>,
}

pub enum ReadingHeader {}
pub enum ReadingDataBlock {}
pub enum Finished {}

pub struct ObjectContainerFsm<'a, State> {
    state: PhantomData<State>,
    inner: InnerObjectContainerFsm<'a>,
}

impl<'a> ObjectContainerFsm<'a, ReadingHeader> {
    pub fn new(schemata: Vec<&'a Schema>) -> Self {
        Self {
            state: PhantomData,
            inner: InnerObjectContainerFsm::Header(HeaderFsm::new(schemata)),
        }
    }
}

impl<'a> Fsm for ObjectContainerFsm<'a, ReadingHeader> {
    type Output = ObjectContainerFsm<'a, ReadingDataBlock>;

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        let InnerObjectContainerFsm::Header(fsm) = self.inner else {
            unreachable!()
        };

        match fsm.parse(buffer)? {
            FsmControlFlow::NeedMore(fsm) => {
                self.inner = InnerObjectContainerFsm::Header(fsm);
                Ok(FsmControlFlow::NeedMore(self))
            }
            FsmControlFlow::Done(header) => Ok(FsmControlFlow::Done(ObjectContainerFsm {
                state: PhantomData::<ReadingDataBlock>,
                inner: InnerObjectContainerFsm::DataBlock(DataBlockFsm::simple_new(header)?),
            })),
        }
    }
}

impl<'a> Fsm for ObjectContainerFsm<'a, ReadingDataBlock> {
    type Output = (Value, Self);

    fn parse(mut self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        let InnerObjectContainerFsm::DataBlock(fsm) = self.inner else {
            unreachable!()
        };

        match fsm.parse(buffer)? {
            FsmControlFlow::NeedMore(fsm) => {
                self.inner = InnerObjectContainerFsm::DataBlock(fsm);
                Ok(FsmControlFlow::NeedMore(self))
            }
            FsmControlFlow::Done((value, fsm)) => {
                self.inner = InnerObjectContainerFsm::DataBlock(fsm);
                Ok(FsmControlFlow::Done((value, self)))
            }
        }
    }
}

impl<'a> ObjectContainerFsm<'a, ReadingDataBlock> {
    /// Get the header of this file.
    pub fn header(&self) -> &Header<'_> {
        let InnerObjectContainerFsm::DataBlock(fsm) = &self.inner else {
            unreachable!()
        };
        fsm.header()
    }

    /// Extract the file header from the partially completed state machine.
    ///
    /// Use [`header`] if you want to continue using the state machine after inspecting the header.
    ///
    /// [`header`]: Self::header
    pub fn into_header(self) -> Header<'a> {
        let InnerObjectContainerFsm::DataBlock(fsm) = self.inner else {
            unreachable!()
        };
        fsm.into_header()
    }

    /// Are we at a block border and is the buffer completely empty.
    ///
    /// If the reader doesn't have more bytes, this indicates that the file has finished parsing.
    /// Otherwise, the reader should continue pushing bytes to the state machine.
    pub fn buffer_empty_at_block_border(&self) -> bool {
        let InnerObjectContainerFsm::DataBlock(fsm) = &self.inner else {
            unreachable!()
        };
        fsm.buffer_empty_at_block_border()
    }
}

enum InnerObjectContainerFsm<'a> {
    Header(HeaderFsm<'a>),
    DataBlock(DataBlockFsm<'a>),
}
