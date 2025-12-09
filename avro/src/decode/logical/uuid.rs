use crate::{
    decode::primitive::bytes::{BytesFsm, FixedFsm, StringFsm},
    error::Details,
    schema::UuidSchema,
    types::Value,
    util::low_level::{Fsm, FsmResult},
};
use oval::Buffer;
use uuid::Uuid;

pub enum UuidFsm {
    String(StringFsm),
    Fixed(FixedFsm),
    Bytes(BytesFsm),
}
impl UuidFsm {
    pub fn new(schema: &UuidSchema) -> Self {
        match schema {
            UuidSchema::String => Self::String(StringFsm::default()),
            UuidSchema::Fixed(fixed_schema) => {
                assert_eq!(fixed_schema.size, 16, "Uuid(Fixed) must be 16 bytes");
                Self::Fixed(FixedFsm::new(16))
            }
            UuidSchema::Bytes => Self::Bytes(BytesFsm::default()),
        }
    }
}
impl Fsm for UuidFsm {
    type Output = Value;

    fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output> {
        match self {
            UuidFsm::String(fsm) => fsm.parse(buffer)?.map_fallible(
                |fsm| Ok(Self::String(fsm)),
                |v| {
                    let Value::String(string) = v else {
                        unreachable!()
                    };
                    Ok(Value::Uuid(
                        Uuid::parse_str(&string).map_err(Details::UuidFromSlice)?,
                    ))
                },
            ),
            UuidFsm::Fixed(fsm) => fsm.parse(buffer)?.map_fallible(
                |fsm| Ok(Self::Fixed(fsm)),
                |v| {
                    let Value::Fixed(16, bytes) = v else {
                        unreachable!()
                    };
                    Ok(Value::Uuid(
                        Uuid::from_slice(&bytes).map_err(Details::UuidFromSlice)?,
                    ))
                },
            ),
            UuidFsm::Bytes(fsm) => fsm.parse(buffer)?.map_fallible(
                |fsm| Ok(Self::Bytes(fsm)),
                |v| {
                    let Value::Bytes(bytes) = v else {
                        unreachable!()
                    };
                    Ok(Value::Uuid(
                        Uuid::from_slice(&bytes).map_err(Details::UuidFromSlice)?,
                    ))
                },
            ),
        }
    }
}
