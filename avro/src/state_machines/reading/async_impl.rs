use std::{collections::HashMap, pin::Pin};
use std::io::Read;
use async_stream::try_stream;
use futures::{AsyncRead, AsyncReadExt, Stream};
use oval::Buffer;
use serde::Deserialize;

use crate::{Error, Schema, error::Details, schema::{Namespace, ResolvedSchema}, state_machines::reading::{
    ItemRead, StateMachine, StateMachineControlFlow,
    commands::CommandTape,
    deserialize_from_tape,
    object_container_file::{
        ObjectContainerFileBodyStateMachine, ObjectContainerFileHeader,
        ObjectContainerFileHeaderStateMachine,
    },
    value_from_tape,
}, types::Value, AvroResult};
use crate::state_machines::reading::datum::DatumStateMachine;

// This should probably also be a state machine and be wrapped in sync and async versions.
// But this suffices for the demonstration.
pub struct Reader<'a, R> {
    reader_schema: Option<&'a Schema>,
    resolved_schemata: ResolvedSchema<'a>,
    header: ObjectContainerFileHeader,
    fsm: Option<ObjectContainerFileBodyStateMachine>,
    reader: R,
    buffer: Buffer,
}

impl<'a, R: AsyncRead + Unpin> Reader<'a, R> {
    /// Creates a [`crate::Reader`] given something implementing the [`Read`] trait to read from.
    ///
    /// No reader [`Schema`] will be set.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the [`crate::Reader`].
    pub async fn new(reader: R) -> Result<Self, Error> {
        Self::new_schemata(reader, Vec::new()).await
    }

    /// Creates a [`crate::Reader`] given a reader [`Schema`] and something implementing the [`Read`] trait
    /// to read from.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the [`crate::Reader`].
    pub async fn with_schema(schema: &'a Schema, reader: R) -> Result<Self, Error> {
        let mut new = Self::new_schemata(reader, Vec::new()).await?;
        new.reader_schema = Some(schema);
        Ok(new)
    }

    /// Creates a [`crate::Reader`] given a reader [`Schema`] and something implementing the [`Read`] trait
    /// to read from.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the [`crate::Reader`].
    pub async fn with_schemata(
        schema: &'a Schema,
        schemata: Vec<&'a Schema>,
        reader: R,
    ) -> Result<Self, Error> {
        let mut new = Self::new_schemata(reader, schemata).await?;
        new.reader_schema = Some(schema);
        Ok(new)
    }

    /// Create a new reader for the Object Container file format.
    ///
    /// This will immediately start reading the header.
    async fn new_schemata(
        mut reader: R,
        schemata: Vec<&'a Schema>,
    ) -> Result<Self, Error> {
        // Read a maximum of 2Kb per read
        let mut buffer = Buffer::with_capacity(2 * 1024);

        // Parse the header
        let rs = ResolvedSchema::try_from(schemata)?;
        let mut fsm = ObjectContainerFileHeaderStateMachine::new(rs.get_names());
        let header = loop {
            // Fill the buffer
            let n = reader
                .read(buffer.space())
                .await
                .map_err(Details::ReadHeader)?;
            if n == 0 {
                return Err(Details::ReadHeader(std::io::ErrorKind::UnexpectedEof.into()).into());
            }
            buffer.fill(n);

            // Start/continue the state machine
            match fsm.parse(&mut buffer)? {
                StateMachineControlFlow::NeedMore(new_fsm) => fsm = new_fsm,
                StateMachineControlFlow::Done(header) => break header,
            }
        };

        let tape = CommandTape::build_from_schema(&header.schema, rs.get_names())?;

        Ok(Self {
            reader_schema: None,
            resolved_schemata: rs,
            fsm: Some(ObjectContainerFileBodyStateMachine::new(
                tape,
                header.sync,
                header.codec,
            )),
            header,
            reader,
            buffer,
        })
    }

    /// Get a reference to the writer [`Schema`].
    pub fn writer_schema(&self) -> &Schema {
        &self.header.schema
    }

    pub fn reader_schema(&self) -> Option<&'a Schema> {
        self.reader_schema
    }

    pub fn user_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.header.metadata
    }

    pub fn header(&self) -> &ObjectContainerFileHeader {
        &self.header
    }

    /// Get the next object in the file
    async fn next_object(&mut self) -> Option<Result<Vec<ItemRead>, Error>> {
        if let Some(mut fsm) = self.fsm.take() {
            loop {
                match fsm.parse(&mut self.buffer) {
                    Ok(StateMachineControlFlow::NeedMore(new_fsm)) => {
                        fsm = new_fsm;
                        let n = match self.reader.read(self.buffer.space()).await {
                            Ok(0) => {
                                return Some(Err(Details::ReadIntoBuf(
                                    std::io::ErrorKind::UnexpectedEof.into(),
                                )
                                .into()));
                            }
                            Ok(n) => n,
                            Err(e) => return Some(Err(Details::ReadIntoBuf(e).into())),
                        };
                        self.buffer.fill(n);
                    }
                    Ok(StateMachineControlFlow::Done(Some((object, fsm)))) => {
                        self.fsm.replace(fsm);
                        return Some(Ok(object));
                    }
                    Ok(StateMachineControlFlow::Done(None)) => {
                        return None;
                    }
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }
        }
        None
    }

    pub async fn stream_serde<'b, T: Deserialize<'b>>(
        &mut self,
    ) -> impl Stream<Item = Result<T, Error>> {
        assert!(
            self.reader_schema.is_none(),
            "Reader schema is not supported with Serde!"
        );
        try_stream! {
            while let Some(object) = self.next_object().await {
                let mut tape = object?;
                yield deserialize_from_tape(&mut tape, &self.header.schema)?;
            }
        }
    }

    pub async fn stream(&mut self) -> impl Stream<Item = Result<Value, Error>> {
        try_stream! {
            while let Some(object) = self.next_object().await {
                let mut tape = object?;

                // TODO: There must be a better way than this
                let rst = ResolvedSchema::new_with_known_schemata(
                    vec![&self.header.schema],
                    &Namespace::None,
                    self.resolved_schemata.get_names(),
                )
                .unwrap();
                let mut names = HashMap::new();
                names.extend(
                    rst.get_names()
                        .iter()
                        .map(|(key, &value)| (key.clone(), value)),
                );
                names.extend(
                    self.resolved_schemata
                        .get_names()
                        .iter()
                        .map(|(key, &value)| (key.clone(), value)),
                );
                let value = value_from_tape(&mut tape, &self.header.schema, &names)?;
                let resolved = if let Some(schema) = self.reader_schema {
                    // TODO: There must be a better way than this
                    let rst = ResolvedSchema::new_with_known_schemata(
                        vec![&self.header.schema],
                        &Namespace::None,
                        self.resolved_schemata.get_names(),
                    )
                    .unwrap();
                    let mut names = HashMap::new();
                    names.extend(
                        rst.get_names()
                            .iter()
                            .map(|(key, &value)| (key.clone(), value)),
                    );
                    names.extend(
                        self.resolved_schemata
                            .get_names()
                            .iter()
                            .map(|(key, &value)| (key.clone(), value)),
                    );
                    value.resolve(schema)?
                } else {
                    value
                };
                yield resolved;
            }
        }
    }
}


/// Decode a raw Avro datum using the provided [`Schema`].
///
/// In case a reader [`Schema`] is provided, schema resolution will be performed.
///
/// **NOTE** This function is very niche and does NOT take care of reading the header and
/// consecutive data blocks. use [`Reader`] if you just want to read an Avro encoded file.
pub async fn from_avro_datum<R: AsyncRead + Unpin>(
    writer_schema: &Schema,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> AvroResult<Value> {
    from_avro_datum_reader_schemata(writer_schema, Vec::new(), reader, reader_schema, Vec::new()).await
}

/// Decode a raw Avro datum using the provided [`Schema`] and schemata.
///
/// If the writer schema contains any [`Schema::Ref`] then it will use the provided
/// schemata to resolve any dependencies.
///
/// In case a reader [`Schema`] is provided, schema resolution will be performed.
pub async fn from_avro_datum_schemata<R: AsyncRead + Unpin>(
    writer_schema: &Schema,
    writer_schemata: Vec<&Schema>,
    reader: &mut R,
    reader_schema: Option<&Schema>,
) -> AvroResult<Value> {
    from_avro_datum_reader_schemata(
        writer_schema,
        writer_schemata,
        reader,
        reader_schema,
        Vec::new(),
    ).await
}

/// Decode a raw Avro datum using the provided [`Schema`] and schemata.
///
/// If the writer schema contains any [`Schema::Ref`] then it will use the provided
/// schemata to resolve any dependencies.
///
/// In case a reader [`Schema`] is provided, schema resolution will be performed.
pub async fn from_avro_datum_reader_schemata<R: AsyncRead + Unpin>(
    writer_schema: &Schema,
    writer_schemata: Vec<&Schema>,
    reader: &mut R,
    reader_schema: Option<&Schema>,
    reader_schemata: Vec<&Schema>,
) -> AvroResult<Value> {
    let rs = ResolvedSchema::try_from(writer_schemata)?;
    let tape = CommandTape::build_from_schema(&writer_schema, &rs.get_names())?;
    // Read a maximum of 2Kb per read
    let mut buffer = Buffer::with_capacity(2 * 1024);
    let mut fsm = DatumStateMachine::new(tape);
    let value = loop {
        // Fill the buffer
        let n = reader.read(buffer.space()).await.map_err(Details::ReadHeader)?;
        if n == 0 {
            return Err(Details::ReadHeader(std::io::ErrorKind::UnexpectedEof.into()).into());
        }
        buffer.fill(n);

        match fsm.parse(&mut buffer)? {
            StateMachineControlFlow::NeedMore(new_fsm) => {
                fsm = new_fsm;
            }
            StateMachineControlFlow::Done(mut tape) => {
                break value_from_tape(&mut tape, &writer_schema, &rs.get_names())?;
            }
        }
    };
    match reader_schema {
        Some(schema) => {
            if reader_schemata.is_empty() {
                value.resolve(schema)
            } else {
                value.resolve_schemata(schema, reader_schemata)
            }
        }
        None => Ok(value),
    }
}
