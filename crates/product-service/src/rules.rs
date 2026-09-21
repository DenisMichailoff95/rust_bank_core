use rust_decimal::Decimal;
use std::str::FromStr;

/// Балансовые счета по типу продукта и валюте.
/// Соответствует российской банковской классификации.
pub fn balance_account_for(product_type: &str) -> &'static str {
    match product_type {
        "current" => "40817",
        "deposit" => "42307",
        "loan" => "455",
        "card" => "40817",
        _ => "40817",
    }
}

/// Код валюты для 20-значного номера счёта.
pub fn currency_code_3(currency: &str) -> &'static str {
    match currency {
        "RUB" => "810",
        "USD" => "840",
        "EUR" => "978",
        "CNY" => "156",
        "KZT" => "398",
        "GBP" => "826",
        "JPY" => "392",
        "CHF" => "756",
        _ => "810",
    }
}

/// Генерация 20-значного номера счёта: балансовый счёт (5) + код валюты (3) + 12 цифр.
pub fn generate_account_number(
    product_type: &str,
    currency_code: &str,
    seq: u64,
) -> String {
    let balance = balance_account_for(product_type);
    let ccy = currency_code_3(currency_code);
    // 12-значная последовательность с ведущими нулями
    let seq_part = format!("{:012}", seq % 1_000_000_000_000);
    format!("{}{}{}", balance, ccy, seq_part)
}

/// Проверка доступности продукта.
pub fn check_eligibility(
    product_type: &str,
    client_status: &str,
    requested_amount: Option<Decimal>,
    terms: &crate::cache::ProductTermsData,
) -> (bool, String) {
    if client_status != "active" {
        return (false, format!("Client status is {} (must be active)", client_status));
    }

    if let Some(amount) = requested_amount {
        let min = Decimal::from_str(&terms.min_amount).unwrap_or(Decimal::ZERO);
        let max = Decimal::from_str(&terms.max_amount).unwrap_or(Decimal::ZERO);

        if amount < min {
            return (false, format!("Amount {} is below minimum {}", amount, min));
        }
        if amount > max {
            return (false, format!("Amount {} exceeds maximum {}", amount, max));
        }
    }

    match product_type {
        "current" | "deposit" | "loan" | "card" => (true, String::new()),
        _ => (false, format!("Unknown product type: {}", product_type)),
    }
}

/// Расчёт персональной ставки.
/// Логика:
/// - Базовая ставка берётся из условий продукта.
/// - Скидка зависит от категории клиента.
/// - Дополнительная скидка за сумму (для крупных сумм).
pub fn calculate_personal_rate(
    base_rate: Decimal,
    client_category: &str,
    requested_amount: Option<Decimal>,
) -> (Decimal, String) {
    let mut rate = base_rate;
    let mut reasons: Vec<String> = Vec::new();

    // Скидка по категории клиента
    match client_category {
        "vip" => {
            rate -= Decimal::from_str("1.5").unwrap();
            reasons.push("VIP-клиент: -1.5%".into());
        }
        "premium" => {
            rate -= Decimal::from_str("0.75").unwrap();
            reasons.push("Premium-клиент: -0.75%".into());
        }
        _ => {}
    }

    // Скидка за крупную сумму
    if let Some(amount) = requested_amount {
        if amount >= Decimal::from_str("1000000").unwrap() {
            rate -= Decimal::from_str("0.5").unwrap();
            reasons.push("Крупная сумма: -0.5%".into());
        } else if amount >= Decimal::from_str("500000").unwrap() {
            rate -= Decimal::from_str("0.25").unwrap();
            reasons.push("Средняя сумма: -0.25%".into());
        }
    }

    // Защита от отрицательной ставки
    if rate < Decimal::from_str("0.01").unwrap() {
        rate = Decimal::from_str("0.01").unwrap();
    }

    rate = rate.round_dp(2);
    (rate, reasons.join("; "))
}