use crate::{
    Codec,
    error::Details,
    state_machines::reading::{StateMachine, StateMachineControlFlow, StateMachineResult},
};
use oval::Buffer;

pub struct CodecStateMachine<T: StateMachine> {
    sub_machine: Option<T>,
    codec: Decoder,
    buffer: Buffer,
}

impl<T: StateMachine> CodecStateMachine<T> {
    pub fn new(sub_machine: T, codec: Codec) -> Self {
        Self {
            sub_machine: Some(sub_machine),
            codec: codec.into(),
            buffer: Buffer::with_capacity(1024),
        }
    }

    pub fn reset(&mut self, sub_machine: T) {
        self.buffer.reset();
        self.sub_machine = Some(sub_machine);
        self.codec.reset();
    }
}

pub enum Decoder {
    Null,
    Deflate(miniz_oxide::inflate::stream::InflateState),
    #[cfg(feature = "snappy")]
    Snappy(snap::raw::Decoder),
    #[cfg(feature = "zstandard")]
    Zstandard(zstd::stream::raw::Decoder<'static>),
    #[cfg(feature = "bzip")]
    Bzip2(bzip2::Decompress),
    #[cfg(feature = "xz")]
    Xz(xz2::stream::Stream),
}

impl From<Codec> for Decoder {
    fn from(value: Codec) -> Self {
        match value {
            Codec::Null => Self::Null,
            Codec::Deflate(_) => Self::Deflate(miniz_oxide::inflate::stream::InflateState::new(
                miniz_oxide::DataFormat::Raw,
            )),
            Codec::Snappy => Self::Snappy(snap::raw::Decoder::new()),
            Codec::Zstandard(_) => Self::Zstandard(zstd::stream::raw::Decoder::new().unwrap()),
            Codec::Bzip2(_) => Self::Bzip2(bzip2::Decompress::new(false)),
            Codec::Xz(_) => Self::Xz(xz2::stream::Stream::new_auto_decoder(u64::MAX, 0).unwrap()),
        }
    }
}

impl Decoder {
    pub fn reset(&mut self) {
        match self {
            Decoder::Null => {}
            Decoder::Deflate(decoder) => {
                decoder.reset_as(miniz_oxide::inflate::stream::MinReset);
            }
            Decoder::Snappy(_decoder) => {} // No reset needed
            Decoder::Zstandard(decoder) => zstd::stream::raw::Operation::reinit(decoder).unwrap(),
            Decoder::Bzip2(decoder) => {
                // No reset/reinit API available
                let _drop = std::mem::replace(decoder, bzip2::Decompress::new(false));
            }
            Decoder::Xz(decoder) => {
                // No reset/reinit API available
                let _drop = std::mem::replace(
                    decoder,
                    xz2::stream::Stream::new_auto_decoder(u64::MAX, 0).unwrap(),
                );
            }
        }
    }
}

impl<T: StateMachine> StateMachine for CodecStateMachine<T> {
    type Output = (Self, T::Output);

    fn parse(mut self, buffer: &mut Buffer) -> StateMachineResult<Self, Self::Output> {
        let buffer = match &mut self.codec {
            Decoder::Null => buffer,
            Decoder::Deflate(decoder) => {
                use miniz_oxide::{MZFlush, StreamResult, inflate::stream::inflate};
                let StreamResult {
                    bytes_consumed,
                    bytes_written,
                    status,
                } = inflate(decoder, buffer.data(), self.buffer.space(), MZFlush::None);
                status.unwrap();
                buffer.consume(bytes_consumed);
                buffer.fill(bytes_written);

                &mut self.buffer
            }
            Decoder::Snappy(decoder) => {
                use snap::raw::decompress_len;
                self.buffer.grow(decompress_len(buffer.data()).unwrap());

                match decoder.decompress(buffer.data(), self.buffer.space()) {
                    Ok(bytes_written) => {
                        self.buffer.fill(bytes_written);
                        todo!("snap does not return the amount of bytes read");
                        &mut self.buffer
                    }
                    Err(snap::Error::HeaderMismatch { .. }) => {
                        // Not enough data yet to decode everything
                        return Ok(StateMachineControlFlow::NeedMore(self));
                    }
                    Err(error) => {
                        return Err(Details::SnappyDecompress(error).into());
                    }
                }
            }
            Decoder::Zstandard(_) => &mut self.buffer,
            Decoder::Bzip2(_) => &mut self.buffer,
            Decoder::Xz(_) => &mut self.buffer,
        };
        match self
            .sub_machine
            .take()
            .expect("CodecStateMachine was not reset!")
            .parse(buffer)?
        {
            StateMachineControlFlow::NeedMore(fsm) => {
                self.sub_machine = Some(fsm);
                Ok(StateMachineControlFlow::NeedMore(self))
            }
            StateMachineControlFlow::Done(result) => {
                Ok(StateMachineControlFlow::Done((self, result)))
            }
        }
    }
}
