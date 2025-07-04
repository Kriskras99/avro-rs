// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{
    schema::{Name, Schema, SchemaKind},
    types::{Value, ValueKind},
};
use std::{error::Error as _, fmt};

#[derive(thiserror::Error)]
pub enum Error {
    #[error("Bad Snappy CRC32; expected {expected:x} but got {actual:x}")]
    SnappyCrc32 { expected: u32, actual: u32 },

    #[error("Invalid u8 for bool: {0}")]
    BoolValue(u8),

    #[error("Not a fixed value, required for decimal with fixed schema: {0:?}")]
    FixedValue(Value),

    #[error("Not a bytes value, required for decimal with bytes schema: {0:?}")]
    BytesValue(Value),

    #[error("Not a string value, required for uuid: {0:?}")]
    GetUuidFromStringValue(Value),

    #[error("Two schemas with the same fullname were given: {0:?}")]
    NameCollision(String),

    #[error("Not a fixed or bytes type, required for decimal schema, got: {0:?}")]
    ResolveDecimalSchema(SchemaKind),

    #[error("Invalid utf-8 string")]
    ConvertToUtf8(#[source] std::string::FromUtf8Error),

    #[error("Invalid utf-8 string")]
    ConvertToUtf8Error(#[source] std::str::Utf8Error),

    /// Describes errors happened while validating Avro data.
    #[error("Value does not match schema")]
    Validation,

    /// Describes errors happened while validating Avro data.
    #[error("Value {value:?} does not match schema {schema:?}: Reason: {reason}")]
    ValidationWithReason {
        value: Value,
        schema: Box<Schema>,
        reason: String,
    },

    #[error("Unable to allocate {desired} bytes (maximum allowed: {maximum})")]
    MemoryAllocation { desired: usize, maximum: usize },

    /// Describe a specific error happening with decimal representation
    #[error(
        "Number of bytes requested for decimal sign extension {requested} is less than the number of bytes needed to decode {needed}"
    )]
    SignExtend { requested: usize, needed: usize },

    #[error("Failed to read boolean bytes: {0}")]
    ReadBoolean(#[source] std::io::Error),

    #[error("Failed to read bytes: {0}")]
    ReadBytes(#[source] std::io::Error),

    #[error("Failed to read string: {0}")]
    ReadString(#[source] std::io::Error),

    #[error("Failed to read double: {0}")]
    ReadDouble(#[source] std::io::Error),

    #[error("Failed to read float: {0}")]
    ReadFloat(#[source] std::io::Error),

    #[error("Failed to read duration: {0}")]
    ReadDuration(#[source] std::io::Error),

    #[error("Failed to read fixed number of bytes '{1}': : {0}")]
    ReadFixed(#[source] std::io::Error, usize),

    #[error("Failed to convert &str to UUID: {0}")]
    ConvertStrToUuid(#[source] uuid::Error),

    #[error("Failed to convert Fixed bytes to UUID. It must be exactly 16 bytes, got {0}")]
    ConvertFixedToUuid(usize),

    #[error("Failed to convert Fixed bytes to UUID: {0}")]
    ConvertSliceToUuid(#[source] uuid::Error),

    #[error("Map key is not a string; key type is {0:?}")]
    MapKeyType(ValueKind),

    #[error("Union index {index} out of bounds: {num_variants}")]
    GetUnionVariant { index: i64, num_variants: usize },

    #[error("Enum symbol index out of bounds: {num_variants}")]
    EnumSymbolIndex { index: usize, num_variants: usize },

    #[error("Enum symbol not found {0}")]
    GetEnumSymbol(String),

    #[error("Unable to decode enum index")]
    GetEnumUnknownIndexValue,

    #[error("Scale {scale} is greater than precision {precision}")]
    GetScaleAndPrecision { scale: usize, precision: usize },

    #[error(
        "Fixed type number of bytes {size} is not large enough to hold decimal values of precision {precision}"
    )]
    GetScaleWithFixedSize { size: usize, precision: usize },

    #[error("Expected Value::Uuid, got: {0:?}")]
    GetUuid(Value),

    #[error("Expected Value::BigDecimal, got: {0:?}")]
    GetBigDecimal(Value),

    #[error("Fixed bytes of size 12 expected, got Fixed of size {0}")]
    GetDecimalFixedBytes(usize),

    #[error("Expected Value::Duration or Value::Fixed(12), got: {0:?}")]
    ResolveDuration(Value),

    #[error("Expected Value::Decimal, Value::Bytes or Value::Fixed, got: {0:?}")]
    ResolveDecimal(Value),

    #[error("Missing field in record: {0:?}")]
    GetField(String),

    #[error("Unable to convert to u8, got {0:?}")]
    GetU8(Value),

    #[error("Precision {precision} too small to hold decimal values with {num_bytes} bytes")]
    ComparePrecisionAndSize { precision: usize, num_bytes: usize },

    #[error("Cannot convert length to i32: {1}")]
    ConvertLengthToI32(#[source] std::num::TryFromIntError, usize),

    #[error("Expected Value::Date or Value::Int, got: {0:?}")]
    GetDate(Value),

    #[error("Expected Value::TimeMillis or Value::Int, got: {0:?}")]
    GetTimeMillis(Value),

    #[error("Expected Value::TimeMicros, Value::Long or Value::Int, got: {0:?}")]
    GetTimeMicros(Value),

    #[error("Expected Value::TimestampMillis, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampMillis(Value),

    #[error("Expected Value::TimestampMicros, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampMicros(Value),

    #[error("Expected Value::TimestampNanos, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampNanos(Value),

    #[error("Expected Value::LocalTimestampMillis, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampMillis(Value),

    #[error("Expected Value::LocalTimestampMicros, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampMicros(Value),

    #[error("Expected Value::LocalTimestampNanos, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampNanos(Value),

    #[error("Expected Value::Null, got: {0:?}")]
    GetNull(Value),

    #[error("Expected Value::Boolean, got: {0:?}")]
    GetBoolean(Value),

    #[error("Expected Value::Int, got: {0:?}")]
    GetInt(Value),

    #[error("Expected Value::Long or Value::Int, got: {0:?}")]
    GetLong(Value),

    #[error(r#"Expected Value::Double, Value::Float, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: {0:?}"#)]
    GetDouble(Value),

    #[error(r#"Expected Value::Float, Value::Double, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: {0:?}"#)]
    GetFloat(Value),

    #[error("Expected Value::Bytes, got: {0:?}")]
    GetBytes(Value),

    #[error("Expected Value::String, Value::Bytes or Value::Fixed, got: {0:?}")]
    GetString(Value),

    #[error("Expected Value::Enum, got: {0:?}")]
    GetEnum(Value),

    #[error("Fixed size mismatch, expected: {size}, got: {n}")]
    CompareFixedSizes { size: usize, n: usize },

    #[error("String expected for fixed, got: {0:?}")]
    GetStringForFixed(Value),

    #[error("Enum default {symbol:?} is not among allowed symbols {symbols:?}")]
    GetEnumDefault {
        symbol: String,
        symbols: Vec<String>,
    },

    #[error("Enum value index {index} is out of bounds {nsymbols}")]
    GetEnumValue { index: usize, nsymbols: usize },

    #[error("Key {0} not found in decimal metadata JSON")]
    GetDecimalMetadataFromJson(&'static str),

    #[error("Could not find matching type in union")]
    FindUnionVariant,

    #[error("Union type should not be empty")]
    EmptyUnion,

    #[error("Array({expected:?}) expected, got {other:?}")]
    GetArray { expected: SchemaKind, other: Value },

    #[error("Map({expected:?}) expected, got {other:?}")]
    GetMap { expected: SchemaKind, other: Value },

    #[error("Record with fields {expected:?} expected, got {other:?}")]
    GetRecord {
        expected: Vec<(String, SchemaKind)>,
        other: Value,
    },

    #[error("No `name` field")]
    GetNameField,

    #[error("No `name` in record field")]
    GetNameFieldFromRecord,

    #[error("Unions may not directly contain a union")]
    GetNestedUnion,

    #[error("Unions cannot contain duplicate types")]
    GetUnionDuplicate,

    #[error("One union type {0:?} must match the `default`'s value type {1:?}")]
    GetDefaultUnion(SchemaKind, ValueKind),

    #[error("`default`'s value type of field {0:?} in {1:?} must be {2:?}")]
    GetDefaultRecordField(String, String, String),

    #[error("JSON value {0} claims to be u64 but cannot be converted")]
    GetU64FromJson(serde_json::Number),

    #[error("JSON value {0} claims to be i64 but cannot be converted")]
    GetI64FromJson(serde_json::Number),

    #[error("Cannot convert u64 to usize: {1}")]
    ConvertU64ToUsize(#[source] std::num::TryFromIntError, u64),

    #[error("Cannot convert u32 to usize: {1}")]
    ConvertU32ToUsize(#[source] std::num::TryFromIntError, u32),

    #[error("Cannot convert i64 to usize: {1}")]
    ConvertI64ToUsize(#[source] std::num::TryFromIntError, i64),

    #[error("Cannot convert i32 to usize: {1}")]
    ConvertI32ToUsize(#[source] std::num::TryFromIntError, i32),

    #[error("Invalid JSON value for decimal precision/scale integer: {0}")]
    GetPrecisionOrScaleFromJson(serde_json::Number),

    #[error("Failed to parse schema from JSON")]
    ParseSchemaJson(#[source] serde_json::Error),

    #[error("Failed to read schema")]
    ReadSchemaFromReader(#[source] std::io::Error),

    #[error("Must be a JSON string, object or array")]
    ParseSchemaFromValidJson,

    #[error("Unknown primitive type: {0}")]
    ParsePrimitive(String),

    #[error("invalid JSON for {key:?}: {value:?}")]
    GetDecimalMetadataValueFromJson {
        key: String,
        value: serde_json::Value,
    },

    #[error("The decimal precision ({precision}) must be bigger or equal to the scale ({scale})")]
    DecimalPrecisionLessThanScale { precision: usize, scale: usize },

    #[error("The decimal precision ({precision}) must be a positive number")]
    DecimalPrecisionMuBePositive { precision: usize },

    #[error("Unreadable big decimal sign")]
    BigDecimalSign,

    #[error("Unreadable length for big decimal inner bytes: {0}")]
    BigDecimalLen(#[source] Box<Error>),

    #[error("Unreadable big decimal scale")]
    BigDecimalScale,

    #[error("Unexpected `type` {0} variant for `logicalType`")]
    GetLogicalTypeVariant(serde_json::Value),

    #[error("No `type` field found for `logicalType`")]
    GetLogicalTypeField,

    #[error("logicalType must be a string, but is {0:?}")]
    GetLogicalTypeFieldType(serde_json::Value),

    #[error("Unknown complex type: {0}")]
    GetComplexType(serde_json::Value),

    #[error("No `type` in complex type")]
    GetComplexTypeField,

    #[error("No `fields` in record")]
    GetRecordFieldsJson,

    #[error("No `symbols` field in enum")]
    GetEnumSymbolsField,

    #[error("Unable to parse `symbols` in enum")]
    GetEnumSymbols,

    #[error("Invalid enum symbol name {0}")]
    EnumSymbolName(String),

    #[error("Invalid field name {0}")]
    FieldName(String),

    #[error("Duplicate field name {0}")]
    FieldNameDuplicate(String),

    #[error("Invalid schema name {0}. It must match the regex '{1}'")]
    InvalidSchemaName(String, &'static str),

    #[error("Invalid namespace {0}. It must match the regex '{1}'")]
    InvalidNamespace(String, &'static str),

    #[error(
        "Invalid schema: There is no type called '{0}', if you meant to define a non-primitive schema, it should be defined inside `type` attribute. Please review the specification"
    )]
    InvalidSchemaRecord(String),

    #[error("Duplicate enum symbol {0}")]
    EnumSymbolDuplicate(String),

    #[error("Default value for enum must be a string! Got: {0}")]
    EnumDefaultWrongType(serde_json::Value),

    #[error("No `items` in array")]
    GetArrayItemsField,

    #[error("No `values` in map")]
    GetMapValuesField,

    #[error("Fixed schema `size` value must be a positive integer: {0}")]
    GetFixedSizeFieldPositive(serde_json::Value),

    #[error("Fixed schema has no `size`")]
    GetFixedSizeField,

    #[error("Fixed schema's default value length ({0}) does not match its size ({1})")]
    FixedDefaultLenSizeMismatch(usize, u64),

    #[error("Failed to compress with flate: {0}")]
    DeflateCompress(#[source] std::io::Error),

    // no longer possible after migration from libflate to miniz_oxide
    #[deprecated(since = "0.19.0", note = "This error can no longer occur")]
    #[error("Failed to finish flate compressor: {0}")]
    DeflateCompressFinish(#[source] std::io::Error),

    #[error("Failed to decompress with flate: {0}")]
    DeflateDecompress(#[source] std::io::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to compress with snappy: {0}")]
    SnappyCompress(#[source] snap::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to get snappy decompression length: {0}")]
    GetSnappyDecompressLen(#[source] snap::Error),

    #[cfg(feature = "snappy")]
    #[error("Failed to decompress with snappy: {0}")]
    SnappyDecompress(#[source] snap::Error),

    #[error("Failed to compress with zstd: {0}")]
    ZstdCompress(#[source] std::io::Error),

    #[error("Failed to decompress with zstd: {0}")]
    ZstdDecompress(#[source] std::io::Error),

    #[error("Failed to read header: {0}")]
    ReadHeader(#[source] std::io::Error),

    #[error("wrong magic in header")]
    HeaderMagic,

    #[error("Message Header mismatch. Expected: {0:?}. Actual: {1:?}")]
    SingleObjectHeaderMismatch(Vec<u8>, Vec<u8>),

    #[error("Failed to get JSON from avro.schema key in map")]
    GetAvroSchemaFromMap,

    #[error("no metadata in header")]
    GetHeaderMetadata,

    #[error("Failed to read marker bytes: {0}")]
    ReadMarker(#[source] std::io::Error),

    #[error("Failed to read block marker bytes: {0}")]
    ReadBlockMarker(#[source] std::io::Error),

    #[error("Read into buffer failed: {0}")]
    ReadIntoBuf(#[source] std::io::Error),

    #[error("block marker does not match header marker")]
    GetBlockMarker,

    #[error("Overflow when decoding integer value")]
    IntegerOverflow,

    #[error("Failed to read bytes for decoding variable length integer: {0}")]
    ReadVariableIntegerBytes(#[source] std::io::Error),

    #[error("Decoded integer out of range for i32: {1}: {0}")]
    ZagI32(#[source] std::num::TryFromIntError, i64),

    #[error("unable to read block")]
    ReadBlock,

    #[error("Failed to serialize value into Avro value: {0}")]
    SerializeValue(String),

    #[error("Failed to serialize value of type {value_type} using schema {schema:?}: {value}")]
    SerializeValueWithSchema {
        value_type: &'static str,
        value: String,
        schema: Box<Schema>,
    },

    #[error("Failed to serialize field '{field_name}' for record {record_schema:?}: {error}")]
    SerializeRecordFieldWithSchema {
        field_name: &'static str,
        record_schema: Box<Schema>,
        error: Box<Error>,
    },

    #[error("Failed to deserialize Avro value into value: {0}")]
    DeserializeValue(String),

    #[error("Failed to write buffer bytes during flush: {0}")]
    WriteBytes(#[source] std::io::Error),

    #[error("Failed to flush inner writer during flush: {0}")]
    FlushWriter(#[source] std::io::Error),

    #[error("Failed to write marker: {0}")]
    WriteMarker(#[source] std::io::Error),

    #[error("Failed to convert JSON to string: {0}")]
    ConvertJsonToString(#[source] serde_json::Error),

    /// Error while converting float to json value
    #[error("failed to convert avro float to json: {0}")]
    ConvertF64ToJson(f64),

    /// Error while resolving Schema::Ref
    #[error("Unresolved schema reference: {0}")]
    SchemaResolutionError(Name),

    #[error("The file metadata is already flushed.")]
    FileHeaderAlreadyWritten,

    #[error("Metadata keys starting with 'avro.' are reserved for internal usage: {0}.")]
    InvalidMetadataKey(String),

    /// Error when two named schema have the same fully qualified name
    #[error("Two named schema defined for same fullname: {0}.")]
    AmbiguousSchemaDefinition(Name),

    #[error("Signed decimal bytes length {0} not equal to fixed schema size {1}.")]
    EncodeDecimalAsFixedError(usize, usize),

    #[error("There is no entry for '{0}' in the lookup table: {1}.")]
    NoEntryInLookupTable(String, String),

    #[error("Can only encode value type {value_kind:?} as one of {supported_schema:?}")]
    EncodeValueAsSchemaError {
        value_kind: ValueKind,
        supported_schema: Vec<SchemaKind>,
    },
    #[error("Internal buffer not drained properly. Re-initialize the single object writer struct!")]
    IllegalSingleObjectWriterState,

    #[error("Codec '{0}' is not supported/enabled")]
    CodecNotSupported(String),

    #[error("Invalid Avro data! Cannot read codec type from value that is not Value::Bytes.")]
    BadCodecMetadata,

    #[error("Cannot convert a slice to Uuid: {0}")]
    UuidFromSlice(#[source] uuid::Error),
}

#[derive(thiserror::Error, PartialEq)]
pub enum CompatibilityError {
    #[error(
        "Incompatible schema types! Writer schema is '{writer_schema_type}', but reader schema is '{reader_schema_type}'"
    )]
    WrongType {
        writer_schema_type: String,
        reader_schema_type: String,
    },

    #[error("Incompatible schema types! The {schema_type} should have been {expected_type:?}")]
    TypeExpected {
        schema_type: String,
        expected_type: Vec<SchemaKind>,
    },

    #[error(
        "Incompatible schemata! Field '{0}' in reader schema does not match the type in the writer schema"
    )]
    FieldTypeMismatch(String, #[source] Box<CompatibilityError>),

    #[error("Incompatible schemata! Field '{0}' in reader schema must have a default value")]
    MissingDefaultValue(String),

    #[error("Incompatible schemata! Reader's symbols must contain all writer's symbols")]
    MissingSymbols,

    #[error("Incompatible schemata! All elements in union must match for both schemas")]
    MissingUnionElements,

    #[error("Incompatible schemata! Name and size don't match for fixed")]
    FixedMismatch,

    #[error(
        "Incompatible schemata! The name must be the same for both schemas. Writer's name {writer_name} and reader's name {reader_name}"
    )]
    NameMismatch {
        writer_name: String,
        reader_name: String,
    },

    #[error(
        "Incompatible schemata! Unknown type for '{0}'. Make sure that the type is a valid one"
    )]
    Inconclusive(String),
}

impl serde::ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::SerializeValue(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::DeserializeValue(msg.to_string())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut msg = self.to_string();
        if let Some(e) = self.source() {
            msg.extend([": ", &e.to_string()]);
        }
        write!(f, "{msg}")
    }
}

impl fmt::Debug for CompatibilityError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut msg = self.to_string();
        if let Some(e) = self.source() {
            msg.extend([": ", &e.to_string()]);
        }
        write!(f, "{msg}")
    }
}
