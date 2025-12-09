/// State machines for logical types.
///
/// # Logical types
/// Avro supports annotating primitive and complex types with additional context. There are currently
/// three categories supported, with thirteen types:
///
/// - decimals: [`decimal`]
/// - time: [`time`]
/// - uuid: [`uuid::UuidFsm`]
pub mod decimal;
pub mod time;
pub mod uuid;
