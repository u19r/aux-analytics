use lru_ttl_cache::{CacheConfig, LruTtlCache};
use regex::Regex;

use crate::{
    condition::CachedCondition,
    constants::CACHE_CAPACITY,
    engine::{AnalyticsEngineError, AnalyticsEngineResult},
    projection::CachedProjection,
};

pub(crate) struct EngineCaches {
    pub projections: LruTtlCache<String, CachedProjection>,
    pub conditions: LruTtlCache<String, CachedCondition>,
    pub regexes: RegexCache,
}

impl EngineCaches {
    pub(crate) fn new() -> Self {
        Self {
            projections: LruTtlCache::new(
                CacheConfig::<String, CachedProjection>::new()
                    .with_capacity(CACHE_CAPACITY)
                    .without_expiration(),
            ),
            conditions: LruTtlCache::new(
                CacheConfig::<String, CachedCondition>::new()
                    .with_capacity(CACHE_CAPACITY)
                    .without_expiration(),
            ),
            regexes: RegexCache::default(),
        }
    }
}

pub(crate) struct RegexCache(LruTtlCache<String, Regex>);

impl Default for RegexCache {
    fn default() -> Self {
        Self(LruTtlCache::new(
            CacheConfig::<String, Regex>::new()
                .with_capacity(CACHE_CAPACITY)
                .without_expiration(),
        ))
    }
}

impl RegexCache {
    pub(crate) fn capture(
        &self,
        attribute_name: &str,
        value: &str,
        regex: &str,
        capture: &str,
    ) -> AnalyticsEngineResult<String> {
        let regex = self.get_regex(attribute_name, regex)?;
        let captures = regex
            .captures(value)
            .ok_or_else(|| AnalyticsEngineError::RegexNoMatch {
                attribute_name: attribute_name.to_string(),
            })?;
        captures
            .name(capture)
            .map(|value| value.as_str().to_string())
            .ok_or_else(|| AnalyticsEngineError::RegexMissingCapture {
                attribute_name: attribute_name.to_string(),
                capture: capture.to_string(),
            })
    }

    fn get_regex(&self, attribute_name: &str, regex: &str) -> AnalyticsEngineResult<regex::Regex> {
        if let Some(regex) = self.0.get_ref(regex) {
            return Ok(regex.clone());
        }
        let compiled =
            regex::Regex::new(regex).map_err(|err| AnalyticsEngineError::InvalidRegex {
                attribute_name: attribute_name.to_string(),
                message: err.to_string(),
            })?;
        self.0.insert(regex.to_string(), compiled.clone());
        Ok(compiled)
    }
}
