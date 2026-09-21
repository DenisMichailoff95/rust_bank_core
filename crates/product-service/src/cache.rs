use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ProductData {
    pub product_id: String,
    pub product_code: String,
    pub product_name: String,
    pub product_type: String,
    pub currency_code: String,
    pub status: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct ProductTermsData {
    pub product_code: String,
    pub min_amount: String,
    pub max_amount: String,
    pub min_term_months: u32,
    pub max_term_months: u32,
    pub base_rate: String,
    pub early_termination_allowed: bool,
    pub early_termination_penalty_days: u32,
    pub capitalization_allowed: bool,
    pub top_up_allowed: bool,
    pub min_balance: String,
}

#[derive(Debug, Clone)]
pub struct TariffPlanData {
    pub tariff_id: String,
    pub product_code: String,
    pub tariff_name: String,
    pub monthly_fee: String,
    pub free_transactions_per_month: u32,
    pub over_limit_fee: String,
    pub grace_period_days: u32,
    pub cashback_percent: String,
}

#[derive(Debug, Default)]
pub struct ProductCache {
    products_by_code: HashMap<String, ProductData>,
    products_by_id: HashMap<String, ProductData>,
    terms_by_code: HashMap<String, ProductTermsData>,
    tariffs_by_id: HashMap<String, TariffPlanData>,
}

impl ProductCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_product_by_code(&self, code: &str) -> Option<&ProductData> {
        self.products_by_code.get(code)
    }

    pub fn get_product_by_id(&self, id: &str) -> Option<&ProductData> {
        self.products_by_id.get(id)
    }

    pub fn list_products(&self, product_type: &str, currency: &str) -> Vec<ProductData> {
        let mut v: Vec<_> = self
            .products_by_code
            .values()
            .filter(|p| {
                (product_type.is_empty() || p.product_type == product_type)
                    && (currency.is_empty() || p.currency_code == currency)
                    && p.status == "active"
            })
            .cloned()
            .collect();
        v.sort_by(|a, b| a.product_code.cmp(&b.product_code));
        v
    }

    pub fn get_terms(&self, code: &str) -> Option<&ProductTermsData> {
        self.terms_by_code.get(code)
    }

    pub fn get_tariff(&self, id: &str) -> Option<&TariffPlanData> {
        self.tariffs_by_id.get(id)
    }

    pub fn replace(
        &mut self,
        products: Vec<ProductData>,
        terms: Vec<ProductTermsData>,
        tariffs: Vec<TariffPlanData>,
    ) {
        self.products_by_code.clear();
        self.products_by_id.clear();
        for p in products {
            self.products_by_code.insert(p.product_code.clone(), p.clone());
            self.products_by_id.insert(p.product_id.clone(), p);
        }

        self.terms_by_code = terms
            .into_iter()
            .map(|t| (t.product_code.clone(), t))
            .collect();

        self.tariffs_by_id = tariffs
            .into_iter()
            .map(|t| (t.tariff_id.clone(), t))
            .collect();
    }

    pub fn products_count(&self) -> usize {
        self.products_by_code.len()
    }

    pub fn terms_count(&self) -> usize {
        self.terms_by_code.len()
    }

    pub fn tariffs_count(&self) -> usize {
        self.tariffs_by_id.len()
    }
}

pub type SharedProductCache = Arc<RwLock<ProductCache>>;

pub fn new_shared_cache() -> SharedProductCache {
    Arc::new(RwLock::new(ProductCache::new()))
}