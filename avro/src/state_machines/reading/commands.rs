use std::collections::HashMap;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use crate::error::Details;
use crate::{Error, Schema};
use crate::schema::{ArraySchema, DecimalSchema, EnumSchema, FixedSchema, MapSchema, Name, RecordSchema, UnionSchema};

/// The next item type that should be read.
#[must_use]
pub enum ToRead {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Enum,
    Ref(CommandTape),
    Fixed(usize),
    Array(CommandTape),
    Map(CommandTape),
    Union(Box<[CommandTape]>),
}

/// A section of a tape of commands.
///
/// This has a reference to the entire tape, so that references to types (for Union,Map,Array) can be resolved.
#[derive(Debug, Clone)]
#[must_use]
pub struct CommandTape {
    inner: Arc<[u8]>,
    read_range: Range<usize>,
}

impl CommandTape {
    /// A reference to a command sequence somewhere else in the tape.
    ///
    /// If the length of the sequence is smaller than or equal to `0xF`, the length is stored in the
    /// four most significant bits of the byte. Otherwise, it's stored as a native endian usize
    /// directly after the command byte. After the length follows the offset as a native endian
    /// usize.
    pub const REF: u8 = 0;
    pub const BOOLEAN: u8 = 1;
    pub const INT: u8 = 2;
    pub const LONG: u8 = 3;
    pub const FLOAT: u8 = 4;
    pub const DOUBLE: u8 = 5;
    pub const BYTES: u8 = 6;
    pub const STRING: u8 = 7;
    pub const ENUM: u8 = 8;
    /// A fixed amount of bytes.
    ///
    /// If the amount of bytes is smaller than or equal to `0xF`, the amount is stored in the four
    /// most significant bits of the byte. Otherwise, it's stored as a native endian usize directly
    /// after the command byte.
    pub const FIXED: u8 = 9;
    /// A block based format follows (i.e. Map or Array).
    ///
    /// The command sequence of the type in the block follows immediately after the command byte.
    /// The length of the sequence is stored in the most significant four bits of the command byte.
    /// If the sequence is larger than `0xF`, then either the entire sequence or part of it is
    /// put behind a [`Self::REF`].
    pub const BLOCK: u8 = 10;
    pub const UNION: u8 = 11;
    /// Skip the next `n` commands.
    ///
    /// If `n` is smaller than or equal to `0xF`, the amount is stored in the four most significant
    /// bits of the byte. Otherwise, it's stored as a native endian usize directly after the command
    /// byte.
    pub const SKIP: u8 = 12;

    /// Create a new tape that will be read from start to end.
    pub fn new(command_tape: Arc<[u8]>) -> Self {
        let length = command_tape.len();
        Self {
            inner: command_tape,
            read_range: 0..length,
        }
    }

    pub fn build_from_schema(schema: &Schema) -> Result<Self, Error> {
        let mut references = HashMap::new();
        let mut tape = Vec::new();
        add_schema_to_tape(&mut tape, schema, &mut references)?;
        let length = tape.len();
        Ok(Self { inner: Arc::from(tape), read_range: 0..length })
    }

    /// Check if the section of the tape we're reading is finished.
    pub fn is_finished(&self) -> bool {
        self.read_range.is_empty()
    }

    /// Extract a part from the tape to give to a sub state machine.
    ///
    /// The tape will run from start to end (inclusive).
    pub fn extract(&self, offset: usize, size: usize) -> Self {
        assert!(
            offset + size <= self.inner.len(),
            "Reference is (partly) outside the tape"
        );
        Self {
            inner: self.inner.clone(),
            read_range: offset..(offset + size),
        }
    }

    /// Extract many parts from the tape to give to the Union state machine.
    ///
    /// The tapes will run from start to end (inclusive).
    pub fn extract_many(&self, parts: &[(usize, usize)]) -> Box<[Self]> {
        let mut vec = Vec::with_capacity(parts.len());
        for &(start, end) in parts {
            vec.push(self.extract(start, end));
        }
        vec.into_boxed_slice()
    }

    /// Read an array of bytes from the tape.
    fn read_array<const N: usize>(&mut self) -> [u8; N] {
        let start = self.read_range.next().expect("Read past the limit");
        let end = self.read_range.nth(N - 1).expect("Read past the limit");
        self.inner[start..=end].try_into().expect("Unreachable!")
    }

    /// Get the next command from the tape.
    ///
    /// # Panics
    /// Will panic if the commands are already finished, see [`Self::is_finished`].
    pub fn command(&mut self) -> ToRead {
        let position = self
            .read_range
            .next()
            .expect("The caller read past the tape");
        let byte = self.inner[position];
        match byte & 0xF {
            Self::REF => {
                // ToRead::Ref
                let size = if byte >> 4 != 0 {
                    // Length is stored inline
                    (byte >> 4) as usize
                } else {
                    usize::from_ne_bytes(self.read_array())
                };
                let offset = usize::from_ne_bytes(self.read_array());
                ToRead::Ref(self.extract(offset, size))
            }
            Self::BOOLEAN => ToRead::Boolean,
            Self::INT => ToRead::Int,
            Self::LONG => ToRead::Long,
            Self::FLOAT => ToRead::Float,
            Self::DOUBLE => ToRead::Double,
            Self::BYTES => ToRead::Bytes,
            Self::STRING => ToRead::String,
            Self::ENUM => ToRead::Enum,
            Self::FIXED => {
                // ToRead::Fixed
                if byte >> 4 != 0 {
                    // Length is stored inline
                    ToRead::Fixed((byte >> 4) as usize)
                } else {
                    let length = usize::from_ne_bytes(self.read_array());
                    ToRead::Fixed(length)
                }
            }
            Self::ARRAY => {
                // ToRead::Array
                // TODO: Use varint for start and length
                let size = if byte >> 4 != 0 {
                    // Length is stored inline
                    (byte >> 4) as usize
                } else {
                    usize::from_ne_bytes(self.read_array())
                };
                let offset = usize::from_ne_bytes(self.read_array());
                ToRead::Array(self.extract(offset, size))
            }
            Self::MAP => {
                // ToRead::Map
                // TODO: If the length of the type is less than 16 we can store the length inline
                let size = if byte >> 4 != 0 {
                    // Length is stored inline
                    (byte >> 4) as usize
                } else {
                    usize::from_ne_bytes(self.read_array())
                };
                let offset = usize::from_ne_bytes(self.read_array());
                ToRead::Map(self.extract(offset, size))
            }
            Self::UNION => {
                // ToRead::Union
                // How many variants are there?
                let number_of_options = if byte >> 4 != 0 {
                    (byte >> 4) as usize
                } else {
                    // TODO: Use varint
                    let number_of_options = u32::from_ne_bytes(self.read_array());
                    number_of_options as usize
                };

                // Where are the references to the variants? Every reference is a pair of usizes.
                // TODO: Use varint
                let position_of_options = usize::from_ne_bytes(self.read_array());

                // Assert that the references are inside the tape.
                assert!(
                    position_of_options + number_of_options * size_of::<(usize, usize)>()
                        < self.inner.len(),
                    "Options are (partly) outside the tape"
                );
                // TODO: Use varint for start and length
                // TODO: Find a safe way to do this
                // SAFETY: As asserted above, the references are entirely inside the slice. The ptr is not null as we've
                //         just read from the slice. We check that the options are aligned before creating the new slice.
                //         The lifetime of the slice is set to the same lifetime as the parent slice.
                let options = unsafe {
                    let options: *const (usize, usize) =
                        self.inner.as_ptr().add(position_of_options).cast();
                    assert!(options.is_aligned());
                    std::slice::from_raw_parts(options, number_of_options)
                };
                ToRead::Union(self.extract_many(options))
            }
            _ => unreachable!(), // TODO: There is room here to specialize certain types, like a Union of Null and some other type
        }
    }
}

fn add_schema_to_tape(tape: &mut Vec<u8>, schema: &Schema, references: &mut HashMap<Name, (usize, usize)>) -> Result<(), Error> {
    match schema {
        Schema::Null => { },
        Schema::Boolean => tape.push(CommandTape::BOOLEAN),
        Schema::Int => tape.push(CommandTape::INT),
        Schema::Long => tape.push(CommandTape::LONG),
        Schema::Float => tape.push(CommandTape::FLOAT),
        Schema::Double => tape.push(CommandTape::DOUBLE),
        Schema::Bytes => tape.push(CommandTape::BYTES),
        Schema::String => tape.push(CommandTape::STRING),
        Schema::Array(ArraySchema { items, .. }) => {
            if let Some(name) = items.name() && let Some((offset, len)) = references.get(&name).copied() {
                // The item schema was already serialized before
                if len == 0 {
                    // An array of nulls?
                    tape.push(CommandTape::BLOCK);
                } else if len <= 0xF {
                    // Inline the value as it's small
                    tape.push(CommandTape::BLOCK | ((len as u8) << 4));
                    tape.extend_from_within(offset..(offset + len));
                } else {
                    // Too large to inline, insert a REF
                    tape.push(CommandTape::BLOCK | 1 << 4);
                    tape.push(CommandTape::REF);
                    tape.extend_from_slice(&len.to_ne_bytes());
                    tape.extend_from_slice(&offset.to_ne_bytes());
                }
            } else {
                // The item schema has not been serialized yet, or is not a named schema
                // As the schema might be arbitrarily large, we insert a SKIP with room for a length
                // we can remove it if it turns out not to be necessary
                let before = tape.len();
                tape.push(CommandTape::SKIP);
                tape.extend_from_slice(&0usize.to_ne_bytes());
                // Make sure we know the offset of the schema
                let schema_offset = tape.len();
                // Serialize the schema to the tape
                add_schema_to_tape(tape, schema, references)?;
                // Calculate the size of the schema
                let schema_len = tape.len() - schema_offset;
                if schema_len {
                    // An array of nulls?
                    tape.truncate(before);
                    tape.push(CommandTape::BLOCK);
                } else if schema_len <= 0xF {
                    // Small enough to inline, replace the SKIP with the BLOCK and remove the 8 length
                    // bytes after SKIP
                    tape[before] = CommandTape::BLOCK | ((schema_len as u8) << 4);
                    tape.copy_within(before + 8..tape.len(), before + 1);
                    // Update the reference if it was added
                    if let Some(name) = items.name() {
                        references.get_mut(&name).expect("Name exists").0 = before + 1;
                    }
                } else {
                    // Too large to inline, update the SKIP with the correct len
                    tape[before + 1..before + 9].copy_from_slice(&schema_len.to_ne_bytes());
                    // Add the BLOCK and REF
                    tape.push(CommandTape::BLOCK | 1 << 4);
                    tape.push(CommandTape::REF);
                    tape.copy_from_slice(&schema_len.to_ne_bytes());
                    tape.copy_from_slice(&schema_offset.to_ne_bytes());
                }
            }
        },
        Schema::Map(MapSchema { types, ..}) => {
            if let Some(name) = types.name() && let Some((offset, len)) = references.get(&name).copied() {
                // The type schema was already serialized before
                if len == 0 {
                    // A map of nulls, so a set?
                    tape.push(CommandTape::BLOCK | 1 << 4);
                    tape.push(CommandTape::STRING);
                } else if len < 0xF {
                    // Inline the value as it's small
                    tape.push(CommandTape::BLOCK | (((len + 1) as u8) << 4));
                    tape.push(CommandTape::STRING);
                    tape.extend_from_within(offset..(offset + len));
                } else {
                    // Too large to inline, insert a REF
                    tape.push(CommandTape::BLOCK | 2 << 4);
                    tape.push(CommandTape::STRING);
                    // Without the STRING the size might fit inline in the REF
                    if len <= 0xF {
                        tape.push(CommandTape::REF | (len as u8) << 4);
                    } else {
                        tape.extend_from_slice(&len.to_ne_bytes());
                    }
                    tape.extend_from_slice(&offset.to_ne_bytes());
                }
            } else {
                // The types schema has not been serialized yet, or is not a named schema
                // As the schema might be arbitrarily large, we insert a SKIP with room for a length
                // we can remove it if it turns out not to be necessary
                let before = tape.len();
                tape.push(CommandTape::SKIP);
                tape.extend_from_slice(&0usize.to_ne_bytes());
                // Make sure we know the offset of the schema
                let schema_offset = tape.len();
                // Serialize the schema to the tape
                add_schema_to_tape(tape, schema, references)?;
                // Calculate the size of the schema
                let schema_len = tape.len() - schema_offset;
                if schema_len {
                    // A map of nulls, so a set?
                    tape.truncate(before);
                    tape.push(CommandTape::BLOCK | (1 << 4));
                    tape.push(CommandTape::STRING);
                } else if schema_len < 0xF {
                    // Small enough to inline, replace the SKIP with the BLOCK and remove the 8 length
                    // bytes after SKIP
                    tape[before] = CommandTape::BLOCK | ((schema_len + 1 as u8) << 4);
                    tape[before + 1] = CommandTape::STRING;
                    tape.copy_within(before + 8..tape.len(), before + 2);
                    // Update the reference if it was added
                    if let Some(name) = types.name() {
                        references.get_mut(&name).expect("Name exists").0 = before + 2;
                    }
                } else {
                    // Too large to inline, update the SKIP with the correct len
                    tape[before + 1..before + 9].copy_from_slice(&schema_len.to_ne_bytes());
                    // Add the BLOCK and REF
                    tape.push(CommandTape::BLOCK | 2 << 4);
                    tape.push(CommandTape::STRING);
                    // Without the STRING the size might fit inline in the REF
                    if schema_len <= 0xF {
                        tape.push(CommandTape::REF | ((schema_len as u8) << 4));
                    } else {
                        tape.push(CommandTape::REF);
                        tape.copy_from_slice(&schema_len.to_ne_bytes());
                    }
                    tape.copy_from_slice(&schema_offset.to_ne_bytes());
                }
            }
        },
        Schema::Union(UnionSchema { schemas, ..}) => {
            let variants = schemas.len();
            let mut schema_refs = Vec::with_capacity(variants);
            for schema in schemas {
                if let Some(name) = schema.name() && let Some(reference) = references.get(name).copied() {
                    schema_refs.push(reference);
                } else {
                    todo!();
                }
            }
            todo!()
        },
        Schema::Record(RecordSchema { fields, name, ..}) => {
            todo!()
        },
        Schema::Enum(EnumSchema { name, ..}) => {
            let index = tape.len();
            tape.push(CommandTape::ENUM);
            references.insert(name.clone(), (index, 1));
        },
        Schema::Fixed(FixedSchema { size, name, .. }) => {
            let size = *size;
            if 0 < size && size <= 0xF {
                let index = tape.len();
                tape.push(CommandTape::FIXED | ((size as u8) << 4));
                references.insert(name.clone(), (index, 1));
            } else {
                let index = tape.len();
                tape.push(CommandTape::FIXED);
                tape.extend_from_slice(&size.to_ne_bytes());
                references.insert(name.clone(), (index, 1 + size_of_val(&size)));
            }
        }
        Schema::Decimal(DecimalSchema { inner, ..}) => add_schema_to_tape(tape, &inner, references)?,
        Schema::BigDecimal => tape.push(CommandTape::BYTES),
        Schema::Uuid => tape.push(CommandTape::STRING),
        Schema::Date => tape.push(CommandTape::INT),
        Schema::TimeMillis => tape.push(CommandTape::INT),
        Schema::TimeMicros => tape.push(CommandTape::LONG),
        Schema::TimestampMillis => tape.push(CommandTape::LONG),
        Schema::TimestampMicros => tape.push(CommandTape::LONG),
        Schema::TimestampNanos => tape.push(CommandTape::LONG),
        Schema::LocalTimestampMillis => tape.push(CommandTape::LONG),
        Schema::LocalTimestampMicros => tape.push(CommandTape::LONG),
        Schema::LocalTimestampNanos => tape.push(CommandTape::LONG),
        Schema::Duration => tape.push(CommandTape::FIXED | 12 << 4),
        Schema::Ref { name } => {
            let Some((start, len)) = references.get(&name).copied() else {
                return Err(Details::SchemaResolutionError(name.clone()).into());
            };
            if len == 0 {
                // Referenced schema only has nulls?
                return Ok(());
            }
            if len <= 0xF {
                tape.push(CommandTape::REF | (len as u8) << 4)
            } else {
                tape.extend_from_slice(&len.to_ne_bytes());
            }
            tape.extend_from_slice(&start.to_ne_bytes());
        }
    }
    Ok(())
}
