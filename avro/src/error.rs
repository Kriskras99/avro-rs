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
    schema::{Name, Schema, SchemaKind, UnionSchema},
    types::{Value, ValueKind},
};
use std::{error::Error as _, fmt};

/// Errors encounterd by Avro.
///
/// To inspect the details of the error use [`details`](Self::details) or [`into_details`](Self::into_details)
/// to get a [`Details`] which contains more precise error information.
///
/// See [`Details`] for all possible errors.
#[derive(thiserror::Error, Debug)]
#[repr(transparent)]
#[error(transparent)]
pub struct Error {
    details: Box<Details>,
}

impl Error {
    pub fn new(details: Details) -> Self {
        Self {
            details: Box::new(details),
        }
    }

    pub fn details(&self) -> &Details {
        &self.details
    }

    pub fn into_details(self) -> Details {
        *self.details
    }
}

impl<T> From<T> for Error
where
    T: Into<Details>,
{
    fn from(value: T) -> Self {
        Self::new(value.into())
    }
}

impl serde::ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::new(<Details as serde::ser::Error>::custom(msg))
    }
}

impl serde::de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::new(<Details as serde::de::Error>::custom(msg))
    }
}

#[derive(thiserror::Error)]
pub enum Details {
    /// Calculated CRC of Snappy decompressed data did not match the included CRC.
    #[error("Bad Snappy CRC32; expected {expected:x} but got {actual:x}")]
    SnappyCrc32 { expected: u32, actual: u32 },

    /// Expected a boolean but got something else than `0` or `1`.
    #[error("Invalid u8 for bool: {0}")]
    BoolValue(u8),

    #[error("Not a fixed value, required for decimal with fixed schema: {0:?}")]
    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    FixedValue(Value),

    #[error("Not a bytes value, required for decimal with bytes schema: {0:?}")]
    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    BytesValue(Value),

    // TODO: Remove
    /// Expected a String for a UUID but got something else.
    #[error("Not a string value, required for uuid: {0:?}")]
    GetUuidFromStringValue(Value),

    /// Got a list of schemas were two schemas had the same fully qualified name.
    #[error("Two schemas with the same fullname were given: {0:?}")]
    NameCollision(String),

    /// Invalid [`Schema::Decimal`] the inner type must be a [`Value::Bytes`] or [`Value::Fixed`].
    #[error("Not a fixed or bytes type, required for decimal schema, got: {0:?}")]
    ResolveDecimalSchema(SchemaKind),

    /// Failed to decode a string as UTF-8.
    #[error("Invalid utf-8 string")]
    ConvertToUtf8(#[from] std::string::FromUtf8Error),

    /// Failed to decode a string as UTF-8.
    #[error("Invalid utf-8 string")]
    ConvertToUtf8Error(#[from] std::str::Utf8Error),

    /// The [`Value`] to encode is not compatible with the provided schema.
    #[error("Value does not match schema")]
    Validation,

    /// The [`Value`] to encode is not compatible with the provided schema.
    #[error("Value {value:?} does not match schema {schema:?}: Reason: {reason}")]
    ValidationWithReason {
        value: Value,
        schema: Schema,
        reason: String,
    },

    /// Decoding the input would result in an allocation larger than allowed.
    ///
    /// The default maximum value is [`DEFAULT_MAX_ALLOCATION_BYTES`](crate::DEFAULT_MAX_ALLOCATION_BYTES),
    /// and can be changed with [`max_allocation_bytes`](crate::max_allocation_bytes).
    #[error("Unable to allocate {desired} bytes (maximum allowed: {maximum})")]
    MemoryAllocation { desired: usize, maximum: usize },

    /// The [`Value::Decimal`] does not fit in the `requested` amount of bytes, needs at least `needed`.
    #[error(
        "Number of bytes requested for decimal sign extension {requested} is less than the number of bytes needed to decode {needed}"
    )]
    SignExtend { requested: usize, needed: usize },

    /// Failed to read a [`Value::Boolean`].
    #[error("Failed to read boolean bytes: {0}")]
    ReadBoolean(#[source] std::io::Error),

    /// Failed to read a [`Value::Bytes`].
    #[error("Failed to read bytes: {0}")]
    ReadBytes(#[source] std::io::Error),

    /// Failed to read a [`Value::String`].
    #[error("Failed to read string: {0}")]
    ReadString(#[source] std::io::Error),

    /// Failed to read a [`Value::Double`].
    #[error("Failed to read double: {0}")]
    ReadDouble(#[source] std::io::Error),

    /// Failed to read a [`Value::Float`].
    #[error("Failed to read float: {0}")]
    ReadFloat(#[source] std::io::Error),

    /// Failed to read a [`Value::Duration`].
    #[error("Failed to read duration: {0}")]
    ReadDuration(#[source] std::io::Error),

    /// Failed to read a [`Value::Fixed`].
    #[error("Failed to read fixed number of bytes '{1}': {0}")]
    ReadFixed(#[source] std::io::Error, usize),

    // TODO: Merge with other uuid::Error?
    /// Failed to parse a UUID from a string.
    #[error("Failed to convert &str to UUID: {0}")]
    ConvertStrToUuid(#[source] uuid::Error),

    // TODO: Remove
    #[error("Failed to convert Fixed bytes to UUID. It must be exactly 16 bytes, got {0}")]
    ConvertFixedToUuid(usize),

    /// Failed to parse a UUID from a byte slice.
    #[error("Failed to convert Fixed bytes to UUID: {0}")]
    ConvertSliceToUuid(#[source] uuid::Error),

    #[error("Map key is not a string; key type is {0:?}")]
    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    MapKeyType(ValueKind),

    /// Decoded union index is larger than the amount of variants.
    #[error("Union index {index} out of bounds: {num_variants}")]
    GetUnionVariant { index: i64, num_variants: usize },

    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    #[error("Enum symbol index out of bounds: {num_variants}")]
    EnumSymbolIndex { index: usize, num_variants: usize },

    /// The string to encode is not a variant of the enum.
    #[error("Enum symbol not found {0}")]
    GetEnumSymbol(String),

    #[error("Unable to decode enum index")]
    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    GetEnumUnknownIndexValue,

    /// Invalid [`Schema::Decimal`], scale is greater than precision.
    #[error("Scale {scale} is greater than precision {precision}")]
    GetScaleAndPrecision { scale: usize, precision: usize },

    /// Invalid [`Schema::Decimal`], the inner [`Schema::Fixed`] is too small to hold values of `precision`.
    #[error(
        "Fixed type number of bytes {size} is not large enough to hold decimal values of precision {precision}"
    )]
    GetScaleWithFixedSize { size: usize, precision: usize },

    // TODO: Split of all the Resolve errors into a separate enum and make a Details::FailedToResolve
    /// Failed to resolve [`Value::Uuid`], expected [`Value::String`] or [`Value::Uuid`] but got something else.
    #[error("Expected Value::Uuid, got: {0:?}")]
    GetUuid(Value),

    /// Failed to resolve [`Value::BigDecimal`], expected [`Value::Bytes`] or [`Value::BigDecimal`] but got something else.
    #[error("Expected Value::BigDecimal, got: {0:?}")]
    GetBigDecimal(Value),

    #[error("Fixed bytes of size 12 expected, got Fixed of size {0}")]
    #[deprecated(since = "0.20.0", note = "Renamed to Details::GetDurationFixedBytes")]
    GetDecimalFixedBytes(usize),

    /// Failed to resolve [`Value::Duration`], got a [`Value::Fixed`] with a size other than 12.
    #[error("Fixed bytes of size 12 expected, got Fixed of size {0}")]
    GetDurationFixedBytes(usize),

    // TODO: Instead of renaming Resolve* to Get*, rename Get* to Resolve* for the Get* that are only returned in the resove functions
    #[error("Expected Value::Duration or Value::Fixed(12), got: {0:?}")]
    #[deprecated(since = "0.20.0", note = "Renamed to Details::GetDuration")]
    ResolveDuration(Value),

    /// Failed to resolve [`Value::Duration`], expected a [`Value::Fixed`] of size 12 or [`Value::Duration`] but got something else.
    #[error("Expected Value::Duration or Value::Fixed(12), got: {0:?}")]
    GetDuration(Value),

    #[error("Expected Value::Decimal, Value::Bytes or Value::Fixed, got: {0:?}")]
    #[deprecated(since = "0.20.0", note = "Renamed to Details::GetDecimal")]
    ResolveDecimal(Value),

    /// Failed to resolve [`Value::Decimal`], expected a [`Value::Bytes`], [`Value::Fixed`] or [`Value::Decimal`] but got something else.
    #[error("Expected Value::Decimal, Value::Bytes or Value::Fixed, got: {0:?}")]
    GetDecimal(Value),

    /// Failed to resolve [`Value::Record`], missing a field which does not have a default.
    #[error("Missing field in record: {0:?}")]
    GetField(String),

    // TODO: Rename to Get* or Resolve*?
    /// Failed to resolve [`Value::Decimal`], the [`Value::Bytes`] or [`Value::Fixed`] is too small to hold values of `precision`.
    #[error("Precision {precision} too small to hold decimal values with {num_bytes} bytes")]
    ComparePrecisionAndSize { precision: usize, num_bytes: usize },

    // TODO: Rename to Get* or Resolve*?
    /// Failed to resolve [`Value::Decimal`], the size of [`Value::Bytes`] or [`Value::Fixed`] is too large.
    #[error("Cannot convert length to i32: {1}")]
    ConvertLengthToI32(#[source] std::num::TryFromIntError, usize),

    /// Failed to resolve [`Value::Date`], expected [`Value::Int`] or [`Value::Date`] but got something else.
    #[error("Expected Value::Date or Value::Int, got: {0:?}")]
    GetDate(Value),

    /// Failed to resolve [`Value::TimeMillis`], expected [`Value::Int`] or [`Value::TimeMillis`] but got something else.
    #[error("Expected Value::TimeMillis or Value::Int, got: {0:?}")]
    GetTimeMillis(Value),

    /// Failed to resolve [`Value::TimeMicros`], expected [`Value::Int`], [`Value::Long`] or [`Value::TimeMicros`] but got something else.
    #[error("Expected Value::TimeMicros, Value::Long or Value::Int, got: {0:?}")]
    GetTimeMicros(Value),

    /// Failed to resolve [`Value::TimestampMillis`], expected [`Value::Int`], [`Value::Long`] or [`Value::TimestampMillis`] but got something else.
    #[error("Expected Value::TimestampMillis, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampMillis(Value),

    /// Failed to resolve [`Value::TimestampMicros`], expected [`Value::Int`], [`Value::Long`] or [`Value::TimestampMicros`] but got something else.
    #[error("Expected Value::TimestampMicros, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampMicros(Value),

    /// Failed to resolve [`Value::TimestampNanos`], expected [`Value::Int`], [`Value::Long`] or [`Value::TimestampNanos`] but got something else.
    #[error("Expected Value::TimestampNanos, Value::Long or Value::Int, got: {0:?}")]
    GetTimestampNanos(Value),

    /// Failed to resolve [`Value::LocalTimestampMillis`], expected [`Value::Int`], [`Value::Long`] or [`Value::LocalTimestampMillis`] but got something else.
    #[error("Expected Value::LocalTimestampMillis, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampMillis(Value),

    /// Failed to resolve [`Value::LocalTimestampMicros`], expected [`Value::Int`], [`Value::Long`] or [`Value::LocalTimestampMicros`] but got something else.
    #[error("Expected Value::LocalTimestampMicros, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampMicros(Value),

    /// Failed to resolve [`Value::LocalTimestampNanos`], expected [`Value::Int`], [`Value::Long`] or [`Value::LocalTimestampNanos`] but got something else.
    #[error("Expected Value::LocalTimestampNanos, Value::Long or Value::Int, got: {0:?}")]
    GetLocalTimestampNanos(Value),

    /// Failed to resolve [`Value::Null`], expected [`Value::Null`] but got something else.
    #[error("Expected Value::Null, got: {0:?}")]
    GetNull(Value),

    /// Failed to resolve [`Value::Boolean`], expected [`Value::Boolean`] but got something else.
    #[error("Expected Value::Boolean, got: {0:?}")]
    GetBoolean(Value),

    /// Failed to resolve [`Value::Int`], expected [`Value::Int`] but got something else.
    #[error("Expected Value::Int, got: {0:?}")]
    GetInt(Value),

    /// Failed to resolve [`Value::Long`], expected [`Value::Int`] or [`Value::Long`] but got something else.
    #[error("Expected Value::Long or Value::Int, got: {0:?}")]
    GetLong(Value),

    /// Failed to resolve [`Value::Double`], expected [`Value::Int`], [`Value::Long`], [`Value::Float`], [`Value::String`] with values `NaN`, `INF`, `Infinity`, `-INF`, `-Infinity`, or [`Value::Double`] but got something else.
    #[error(r#"Expected Value::Double, Value::Float, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: {0:?}"#)]
    GetDouble(Value),

    /// Failed to resolve [`Value::Float`], expected [`Value::Int`], [`Value::Long`], [`Value::Doube`], [`Value::String`] with values `NaN`, `INF`, `Infinity`, `-INF`, `-Infinity`, or [`Value::Float`] but got something else.
    #[error(r#"Expected Value::Float, Value::Double, Value::Int, Value::Long or Value::String ("NaN", "INF", "Infinity", "-INF" or "-Infinity"), got: {0:?}"#)]
    GetFloat(Value),

    /// Failed to resolve [`Value::Bytes`], expected a [`Value::Array`] of integers, [`Value::String`] or [`Value::Bytes`].
    #[error("Expected Value::Bytes, got: {0:?}")]
    GetBytes(Value),

    /// Failed to resolve [`Value::Bytes`], got a [`Value::Array`] of integers but one of the values is outside the valid range for `u8`.
    #[error("Unable to convert to u8, got {0:?}")]
    GetU8(i32),

    /// Failed to resolve [`Value::String`], expected [`Value::Bytes`], [`Value::Fixed`] or [`Value::String`] but got something else.
    #[error("Expected Value::String, Value::Bytes or Value::Fixed, got: {0:?}")]
    GetString(Value),

    /// Failed to resolve [`Value::Enum`], expected [`Value::Enum`] but got something else.
    #[error("Expected Value::Enum, got: {0:?}")]
    GetEnum(Value),

    // TODO: Move this away from the resolve errors
    /// Failed to serialize [`Value::Decimal`], the [`Value::Fixed`] size does not match the decimal length.
    #[error("Fixed size mismatch, expected: {size}, got: {n}")]
    CompareFixedSizes { size: usize, n: usize },

    #[error("String expected for fixed, got: {0:?}")]
    #[deprecated(since = "0.20.0", note = "Renamed to Details::GetFixed")]
    GetStringForFixed(Value),

    /// Failed to resolve [`Value::Fixed`], expected [`Value::Bytes`], [`Value::Fixed`] or [`Value::String`] of `size` bytes but got something else.
    #[error("Expected Value::Fixed, Value::Bytes or Value::String of size {size}, got: {got:?}")]
    GetFixed { size: usize, got: Value },

    /// The enum default value is not on the list of symbols.
    #[error("Enum default {symbol:?} is not among allowed symbols {symbols:?}")]
    GetEnumDefault {
        symbol: String,
        symbols: Vec<String>,
    },

    /// Failed to decode [`Value::Enum`], the index is larger than the amount of symbols.
    #[error("Enum value index {index} is out of bounds {nsymbols}")]
    GetEnumValue { index: usize, nsymbols: usize },

    /// Precision key not in Decimal JSON.
    #[error("Precision not found in decimal metadata JSON")]
    GetDecimalMetadataFromJson,

    /// Failed to resolve [`Value::Union`], `value` does not match any of the variants.
    #[error("Could not find matching type in {schema:?} for {value:?}")]
    FindUnionVariant { schema: UnionSchema, value: Value },

    /// Union doesn't have any variants.
    #[error("Union type should not be empty")]
    EmptyUnion,

    /// Failed to resolve [`Value::Array`], expected [`Value::Array`] but got something else.
    #[error("Array({expected:?}) expected, got {other:?}")]
    GetArray { expected: SchemaKind, other: Value },

    /// Failed to resolve [`Value::Map`], expected [`Value::Map`] but got something else.
    #[error("Map({expected:?}) expected, got {other:?}")]
    GetMap { expected: SchemaKind, other: Value },

    /// Failed to resolve [`Value::Record`], expected [`Value::Map`] or [`Value::Record`] but got something else.
    #[error("Record with fields {expected:?} expected, got {other:?}")]
    GetRecord {
        expected: Vec<(String, SchemaKind)>,
        other: Value,
    },

    /// No `name` field in the schema.
    #[error("No `name` field")]
    GetNameField,

    /// No `name` field in the record field schema.
    #[error("No `name` in record field")]
    GetNameFieldFromRecord,

    /// Union schema contains a directly nested union.
    #[error("Unions may not directly contain a union")]
    GetNestedUnion,

    /// Union schema contains duplicate types.
    #[error("Unions cannot contain duplicate types")]
    GetUnionDuplicate,

    /// The union default does not match the type of any of the variants.
    #[error("One union type {0:?} must match the `default`'s value type {1:?}")]
    GetDefaultUnion(SchemaKind, ValueKind),

    /// The default of a record field does not match the type of the record field.
    #[error("`default`'s value type of field {0:?} in {1:?} must be {2:?}")]
    GetDefaultRecordField(String, String, String),

    #[error("JSON value {0} claims to be u64 but cannot be converted")]
    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    GetU64FromJson(serde_json::Number),

    #[error("JSON value {0} claims to be i64 but cannot be converted")]
    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    GetI64FromJson(serde_json::Number),

    /// Failed to convert a `u64` to `usize`.
    #[error("Cannot convert u64 to usize: {1}")]
    ConvertU64ToUsize(#[source] std::num::TryFromIntError, u64),

    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    #[error("Cannot convert u32 to usize: {1}")]
    ConvertU32ToUsize(#[source] std::num::TryFromIntError, u32),

    /// Failed to convert a `i64` to `usize`.
    #[error("Cannot convert i64 to usize: {1}")]
    ConvertI64ToUsize(#[source] std::num::TryFromIntError, i64),

    /// Failed to convert a `i32` to `usize`.
    #[error("Cannot convert i32 to usize: {1}")]
    ConvertI32ToUsize(#[source] std::num::TryFromIntError, i32),

    /// Decimal schema precision and/or scale is not a `u64` or `i64`.
    #[error("Invalid JSON value for decimal precision/scale integer: {0}")]
    GetPrecisionOrScaleFromJson(serde_json::Number),

    /// Schema is not valid JSON.
    #[error("Failed to parse schema from JSON")]
    ParseSchemaJson(#[source] serde_json::Error),

    /// Failed to read the schema.
    #[error("Failed to read schema")]
    ReadSchemaFromReader(#[source] std::io::Error),

    /// Failed to parse the schema.
    #[error("Must be a JSON string, object or array")]
    ParseSchemaFromValidJson,

    #[error("Unknown primitive type: {0}")]
    #[deprecated(since = "0.20.0", note = "Renamed to Details::FindNamedReference")]
    ParsePrimitive(String),

    /// Failed to find named schema for a [`Schema::Ref`].
    #[error("Failed to find named reference: {0}")]
    FindNamedSchema(String),

    /// Failed to parse a [`Schema::Decimal`], expected a [`Value::Number`](serde_json::Value::Number) for `key` but got something else.
    #[error("invalid JSON for {key:?}: {value:?}")]
    GetDecimalMetadataValueFromJson {
        key: String,
        value: serde_json::Value,
    },

    /// Failed to parse a [`Schema::Decimal`], `precision` must be greater than or equal to `scale`.
    #[error("The decimal precision ({precision}) must be bigger or equal to the scale ({scale})")]
    DecimalPrecisionLessThanScale { precision: usize, scale: usize },

    /// Failed to parse a [`Schema::Decimal`], `precision` must be greater than or equal to `1`.
    #[error("The decimal precision ({precision}) must be a positive number")]
    DecimalPrecisionMuBePositive { precision: usize },

    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    #[error("Unreadable big decimal sign")]
    BigDecimalSign,

    // TODO: Is this useful on top of the error returned by parse_len?
    /// Failed to decode a [`Value::BigDecimal`], the length is unreadable.
    #[error("Unreadable length for big decimal inner bytes: {0}")]
    BigDecimalLen(#[source] Box<Error>),

    // TODO: Why do we include the source for BigDecimalLen but not for BigDecimalScale?
    /// Failed to decode a [`Value::BigDecimal`], the scale is unreadable.
    #[error("Unreadable big decimal scale")]
    BigDecimalScale,

    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
    #[error("Unexpected `type` {0} variant for `logicalType`")]
    GetLogicalTypeVariant(serde_json::Value),

    /// Failed to parse the schema, a `logicalType` is missing the `type` field.
    #[error("No `type` field found for `logicalType`")]
    GetLogicalTypeField,

    /// Failed to parse the schema, a `logicalType` has a `type` field but it's not a string.
    #[error("logicalType must be a string, but is {0:?}")]
    GetLogicalTypeFieldType(serde_json::Value),

    /// Failed to parse the schema, expected a [`Value::String`], [`Value::Array`] or [`Value::Object`] but got something else.
    ///
    /// [`Value::String`](serde_json::Value::String)
    /// [`Value::Array`](serde_json::Value::Array)
    /// [`Value::Object`](serde_json::Value::Object)
    #[error("Unknown complex type: {0}")]
    GetComplexType(serde_json::Value),

    /// Failed to parse the schema, missing `type` field.
    #[error("No `type` in complex type")]
    GetComplexTypeField,

    /// Failed to parse the schema, a record is missing the `fields` field.
    #[error("No `fields` in record")]
    GetRecordFieldsJson,

    /// Failed to parse the schema, a enum is missing the `symbols` field
    #[error("No `symbols` field in enum")]
    GetEnumSymbolsField,

    /// Failed to parse the schema, the symbols in the `symbols` field are not strings.
    #[error("Unable to parse `symbols` in enum")]
    GetEnumSymbols,

    /// Failed to parse the schema, a symbol in the `symbols` field has an invalid name.
    ///
    /// See [`set_enum_symbol_name_validator`](crate::validator::set_enum_symbol_name_validator).
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

    #[deprecated(since = "0.20.0", note = "This error variant is not generated anymore")]
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

    #[error(
        "Invalid sync marker! The sync marker in the data block \
        doesn't match the file header's sync marker. This likely \
        indicates data corruption, truncated file, or incorrectly \
        concatenated Avro files. Verify file integrity and ensure \
        proper file transmission or creation."
    )]
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
        schema: Schema,
    },

    #[error("Failed to serialize field '{field_name}' for record {record_schema:?}: {error}")]
    SerializeRecordFieldWithSchema {
        field_name: &'static str,
        record_schema: Schema,
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
    #[deprecated(since = "0.20", note = "Merged with Details::ConvertSliceToUuid")]
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

impl serde::ser::Error for Details {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Details::SerializeValue(msg.to_string())
    }
}

impl serde::de::Error for Details {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Details::DeserializeValue(msg.to_string())
    }
}

impl fmt::Debug for Details {
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
