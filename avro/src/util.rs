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

//! Utility functions, like configuring various global settings.

use crate::{AvroResult, error::Details, schema::Documentation};
use serde_json::{Map, Value};
use std::sync::OnceLock;

/// Maximum number of bytes that can be allocated when decoding
/// Avro-encoded values. This is a protection against ill-formed
/// data, whose length field might be interpreted as enormous.
/// See max_allocation_bytes to change this limit.
pub const DEFAULT_MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;
static MAX_ALLOCATION_BYTES: OnceLock<usize> = OnceLock::new();

/// Whether to set serialization & deserialization traits
/// as `human_readable` or not.
/// See [set_serde_human_readable] to change this value.
// crate-visible for testing
pub(crate) static SERDE_HUMAN_READABLE: OnceLock<bool> = OnceLock::new();
/// Whether the serializer and deserializer should indicate to types that the format is human-readable.
pub const DEFAULT_SERDE_HUMAN_READABLE: bool = false;

/// Set the maximum number of bytes that can be allocated when decoding data.
///
/// This function only changes the setting once. On subsequent calls the value will stay the same
/// as the first time it is called. It is automatically called on first allocation and defaults to
/// [`DEFAULT_MAX_ALLOCATION_BYTES`].
///
/// # Returns
/// The configured maximum, which might be different from what the function was called with if the
/// value was already set before.
pub fn max_allocation_bytes(num_bytes: usize) -> usize {
    *MAX_ALLOCATION_BYTES.get_or_init(|| num_bytes)
}

pub(crate) fn safe_len(len: usize) -> AvroResult<usize> {
    let max_bytes = max_allocation_bytes(DEFAULT_MAX_ALLOCATION_BYTES);

    if len <= max_bytes {
        Ok(len)
    } else {
        Err(Details::MemoryAllocation {
            desired: len,
            maximum: max_bytes,
        }
        .into())
    }
}

/// Set whether the serializer and deserializer should indicate to types that the format is human-readable.
///
/// This function only changes the setting once. On subsequent calls the value will stay the same
/// as the first time it is called. It is automatically called on first allocation and defaults to
/// [`DEFAULT_SERDE_HUMAN_READABLE`].
///
/// *NOTE*: Changing this setting can change the output of [`from_value`](crate::from_value) and the
/// accepted input of [`to_value`](crate::to_value).
///
/// # Returns
/// The configured human-readable value, which might be different from what the function was called
/// with if the value was already set before.
pub fn set_serde_human_readable(human_readable: bool) -> bool {
    *SERDE_HUMAN_READABLE.get_or_init(|| human_readable)
}

pub(crate) fn is_human_readable() -> bool {
    *SERDE_HUMAN_READABLE.get_or_init(|| DEFAULT_SERDE_HUMAN_READABLE)
}

/// Utilities for working with low-level parts of the library like [`encode`] and [`decode`].
///
/// [`encode`]: crate::encode
/// [`decode`]: crate::decode
pub mod low_level {
    use crate::Error;
    use oval::Buffer;

    /// A trait for the lifecycle of a finite state machine.
    pub trait Fsm: Sized {
        /// The final output of the state machine.
        type Output: Sized;

        /// Start/continue the state machine.
        ///
        /// Implementers are not allowed to return until they can't make progress anymore.
        fn parse(self, buffer: &mut Buffer) -> FsmResult<Self, Self::Output>;
    }

    /// Indicates whether the state machine has completed or needs to be polled again.
    #[must_use]
    pub enum FsmControlFlow<Fsm, Output> {
        /// The state machine needs more data before it can continue.
        NeedMore(Fsm),
        /// The state machine is done and the result is returned.
        Done(Output),
    }

    impl<FSM1, O1> FsmControlFlow<FSM1, O1> {
        /// Map a state machine to another state machine.
        ///
        /// This function will only execute `need_more` or `done`, not both.
        pub fn map<F1, F2, FSM2, O2>(self, need_more: F1, done: F2) -> FsmControlFlow<FSM2, O2>
        where
            F1: FnOnce(FSM1) -> FSM2,
            F2: FnOnce(O1) -> O2,
        {
            match self {
                FsmControlFlow::NeedMore(fsm) => FsmControlFlow::NeedMore(need_more(fsm)),
                FsmControlFlow::Done(fsm) => FsmControlFlow::Done(done(fsm)),
            }
        }

        /// Map a state machine to another state machine with fallible conversions.
        ///
        /// This function will only execute `need_more` or `done`, not both.
        pub fn map_fallible<F1, F2, FSM2, O2>(self, need_more: F1, done: F2) -> FsmResult<FSM2, O2>
        where
            F1: FnOnce(FSM1) -> Result<FSM2, Error>,
            F2: FnOnce(O1) -> Result<O2, Error>,
        {
            match self {
                FsmControlFlow::NeedMore(fsm) => Ok(FsmControlFlow::NeedMore(need_more(fsm)?)),
                FsmControlFlow::Done(fsm) => Ok(FsmControlFlow::Done(done(fsm)?)),
            }
        }
    }

    /// The result of an execution of a state machine.
    pub type FsmResult<Fsm, Output> = Result<FsmControlFlow<Fsm, Output>, Error>;
}

pub(crate) trait MapHelper {
    fn string(&self, key: &str) -> Option<String>;

    fn name(&self) -> Option<String> {
        self.string("name")
    }

    fn doc(&self) -> Documentation {
        self.string("doc")
    }

    fn aliases(&self) -> Option<Vec<String>>;
}

impl MapHelper for Map<String, Value> {
    fn string(&self, key: &str) -> Option<String> {
        self.get(key)
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    }

    fn aliases(&self) -> Option<Vec<String>> {
        // FIXME no warning when aliases aren't a json array of json strings
        self.get("aliases")
            .and_then(|aliases| aliases.as_array())
            .and_then(|aliases| {
                aliases
                    .iter()
                    .map(|alias| alias.as_str())
                    .map(|alias| alias.map(|a| a.to_string()))
                    .collect::<Option<_>>()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro_test_helper::TestResult;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_safe_len() -> TestResult {
        assert_eq!(42usize, safe_len(42usize)?);
        assert!(safe_len(1024 * 1024 * 1024).is_err());

        Ok(())
    }
}
