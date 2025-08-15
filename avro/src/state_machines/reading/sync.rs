use std::{collections::HashMap, io::Read};

use oval::Buffer;
use serde::Deserialize;

use crate::{
    Error, Schema,
    error::Details,
    schema::{Namespace, ResolvedSchema},
    state_machines::reading::{
        ItemRead, StateMachine, StateMachineControlFlow,
        commands::CommandTape,
        deserialize_from_tape,
        object_container_file::{
            ObjectContainerFileBodyStateMachine, ObjectContainerFileHeader,
            ObjectContainerFileHeaderStateMachine,
        },
        value_from_tape,
    },
    types::Value,
};

pub struct ObjectContainerFileReader<'a, R> {
    reader_schema: Option<&'a Schema>,
    resolved_schemata: ResolvedSchema<'a>,
    header: ObjectContainerFileHeader,
    fsm: Option<ObjectContainerFileBodyStateMachine>,
    reader: R,
    buffer: Buffer,
}

impl<'a, R: Read> ObjectContainerFileReader<'a, R> {
    /// Create a new reader for the Object Container file format.
    pub fn new(reader: R) -> Result<Self, Error> {
        Self::new_schemata(reader, Vec::new())
    }

    pub fn with_schema(schema: &'a Schema, reader: R) -> Result<Self, Error> {
        let mut new = Self::new_schemata(reader, Vec::new())?;
        new.reader_schema = Some(schema);
        Ok(new)
    }

    pub fn with_schemata(
        schema: &'a Schema,
        schemata: Vec<&'a Schema>,
        reader: R,
    ) -> Result<Self, Error> {
        let mut new = Self::new_schemata(reader, schemata)?;
        new.reader_schema = Some(schema);
        Ok(new)
    }

    fn new_schemata(mut reader: R, schemata: Vec<&'a Schema>) -> Result<Self, Error> {
        // Read a maximum of 2Kb per read
        let mut buffer = Buffer::with_capacity(2 * 1024);

        // Parse the header
        let rs = ResolvedSchema::try_from(schemata)?;
        let mut fsm = ObjectContainerFileHeaderStateMachine::new(rs.get_names());
        let header = loop {
            // Fill the buffer
            let n = reader.read(buffer.space()).map_err(Details::ReadHeader)?;
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
    fn next_object(&mut self) -> Option<Result<Vec<ItemRead>, Error>> {
        if let Some(mut fsm) = self.fsm.take() {
            loop {
                match fsm.parse(&mut self.buffer) {
                    Ok(StateMachineControlFlow::NeedMore(new_fsm)) => {
                        fsm = new_fsm;
                        let n = match self.reader.read(self.buffer.space()) {
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

    pub fn next_serde<'b, T: Deserialize<'b>>(&mut self) -> Option<Result<T, Error>> {
        assert!(
            self.reader_schema.is_none(),
            "Reader schema is not supported with Serde!"
        );
        self.next_object()
            .map(|r| r.and_then(|mut tape| deserialize_from_tape(&mut tape, &self.header.schema)))
    }
}

impl<R: Read> Iterator for ObjectContainerFileReader<'_, R> {
    type Item = Result<Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_object().map(|r| {
            r.and_then(|mut tape| {
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
                value_from_tape(&mut tape, &self.header.schema, &names)
            })
            .and_then(|v| {
                if let Some(schema) = &self.reader_schema {
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
                    v.resolve_internal(schema, &names, &Namespace::None, &None)
                } else {
                    Ok(v)
                }
            })
        })
    }
}
