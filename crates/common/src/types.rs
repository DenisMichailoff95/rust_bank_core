use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Money {
    pub amount: String,       // Decimal как строка
    pub currency_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionDirection {
    Debit,
    Credit,
}