// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use clocksource::Nanoseconds;

pub type Duration = clocksource::Duration<Nanoseconds<u64>>;
pub type Instant = clocksource::Instant<Nanoseconds<u64>>;
