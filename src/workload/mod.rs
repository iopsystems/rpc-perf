// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

mod item;

pub mod stats;

pub use item::WorkItem;

use rand::Rng;
use rand::SeedableRng;
use rand::distributions::Alphanumeric;
use rand_xoshiro::Xoshiro256Plus;
use std::sync::Arc;
use rand::prelude::Distribution;



#[derive(Clone)]
pub struct Keyspace<T>
where
    T: Distribution<usize>,
{
    keys: Arc<Box<[Arc<String>]>>,
    distribution: Box<T>,
    cardinality: Option<Box<T>>,
    rng: Xoshiro256Plus,
}

#[derive(Clone)]
pub struct InnerKeyspace<T>
where
    T: Distribution<usize>,
{
    inner: Keyspace<T>,
}

impl<T> InnerKeyspace<T>
where
    T: Distribution<usize>,
{
    pub fn new(klen: usize, count: usize, distribution: Box<T>, cardinality: Option<Box<T>>) -> Self {
        Self {
            inner: Keyspace::new(klen, count, distribution, cardinality),
        }
    }

    pub fn sample(&mut self) -> Arc<String> {
        self.inner.sample()
    }

    pub fn multi_sample(&mut self) -> Vec<Arc<String>> {
        self.inner.multi_sample()
    }
}

impl<T> Keyspace<T>
where
    T: Distribution<usize>,
{
    pub fn new(klen: usize, count: usize, distribution: Box<T>, cardinality: Option<Box<T>>) -> Self {
        let mut rng = Xoshiro256Plus::seed_from_u64(0);

        let mut keys = Vec::with_capacity(count);

        for _ in 0..count {
            let key: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(klen)
                .map(char::from)
                .collect();
            keys.push(Arc::new(key));
        }
        Keyspace {
            keys: Arc::new(keys.into_boxed_slice()),
            distribution,
            cardinality,
            rng,
        }
    }

    pub fn sample(&mut self) -> Arc<String> {
        let idx = self.distribution.sample(&mut self.rng);
        self.keys[idx].clone()
    }

    pub fn multi_sample(&mut self) -> Vec<Arc<String>> {
        let cardinality = if let Some(c) = &self.cardinality {
            c.sample(&mut self.rng)
        } else {
            1
        };

        let mut samples = Vec::with_capacity(cardinality);
        for _ in 0..cardinality {
            let idx = self.distribution.sample(&mut self.rng);
            samples.push(self.keys[idx].clone())
        }
        samples
    }
}