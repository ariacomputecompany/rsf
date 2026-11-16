//! Real owner boundaries; these tests do not simulate machine power loss.
use super::*;
use crate::learner::prefix::observation::{self, Boundary};
mod fixture;
use fixture::*;
mod duplicate;
mod renewal;
