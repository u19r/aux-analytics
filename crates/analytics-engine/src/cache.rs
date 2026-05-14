use std::{collections::HashMap, hash::Hash};

use regex::Regex;

use crate::condition::CachedCondition;

pub(crate) const CACHE_CAPACITY: usize = 10_000;

#[derive(Debug)]
pub(crate) struct EngineCaches {
    pub(crate) conditions: LruCache<String, CachedCondition>,
    pub(crate) regexes: LruCache<String, Regex>,
}

impl EngineCaches {
    pub(crate) fn new() -> Self {
        Self {
            conditions: LruCache::new(CACHE_CAPACITY),
            regexes: LruCache::new(CACHE_CAPACITY),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LruCache<K, V> {
    capacity: usize,
    tick: u64,
    entries: HashMap<K, CacheEntry<V>>,
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            tick: 0,
            entries: HashMap::with_capacity(capacity.min(1024)),
        }
    }

    pub(crate) fn get(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.get_mut(key)?;
        self.tick = self.tick.wrapping_add(1);
        entry.last_used = self.tick;
        Some(entry.value.clone())
    }

    pub(crate) fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }
        self.tick = self.tick.wrapping_add(1);
        self.entries.insert(
            key,
            CacheEntry {
                value,
                last_used: self.tick,
            },
        );
        if self.entries.len() > self.capacity {
            self.evict_least_recently_used();
        }
    }

    fn evict_least_recently_used(&mut self) {
        let Some(key) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
        else {
            return;
        };
        self.entries.remove(&key);
    }
}

#[derive(Debug)]
struct CacheEntry<V> {
    value: V,
    last_used: u64,
}
