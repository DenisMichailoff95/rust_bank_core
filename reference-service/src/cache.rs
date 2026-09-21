use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct Currency {
    pub currency_code: String,
    pub numeric_code: u32,
    pub name: String,
    pub decimal_places: u32,
    pub is_base: bool,
}

#[derive(Debug, Clone)]
pub struct OperationType {
    pub operation_code: String,
    pub name: String,
    pub direction: String,
}

#[derive(Debug, Default)]
pub struct ReferenceCache {
    currencies: HashMap<String, Currency>,
    operation_types: HashMap<String, OperationType>,
}

impl ReferenceCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_currency(&self, code: &str) -> Option<&Currency> {
        self.currencies.get(code)
    }

    pub fn list_currencies(&self) -> Vec<Currency> {
        let mut v: Vec<_> = self.currencies.values().cloned().collect();
        v.sort_by(|a, b| a.currency_code.cmp(&b.currency_code));
        v
    }

    pub fn get_operation_type(&self, code: &str) -> Option<&OperationType> {
        self.operation_types.get(code)
    }

    pub fn list_operation_types(&self) -> Vec<OperationType> {
        let mut v: Vec<_> = self.operation_types.values().cloned().collect();
        v.sort_by(|a, b| a.operation_code.cmp(&b.operation_code));
        v
    }

    pub fn replace(
        &mut self,
        currencies: Vec<Currency>,
        operation_types: Vec<OperationType>,
    ) {
        self.currencies = currencies
            .into_iter()
            .map(|c| (c.currency_code.clone(), c))
            .collect();
        self.operation_types = operation_types
            .into_iter()
            .map(|o| (o.operation_code.clone(), o))
            .collect();
    }

    pub fn currencies_count(&self) -> usize {
        self.currencies.len()
    }

    pub fn operation_types_count(&self) -> usize {
        self.operation_types.len()
    }
}

pub type SharedCache = Arc<RwLock<ReferenceCache>>;

pub fn new_shared_cache() -> SharedCache {
    Arc::new(RwLock::new(ReferenceCache::new()))
}