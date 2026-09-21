use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use ydb::{Client, Query, YdbResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositAccountRecord {
    pub deposit_account_id: String,
    pub deposit_id: String,
    pub client_id: String,
    pub currency_code: String,
    pub account_number: String,
    pub principal_balance: String,
    pub interest_accrued: String,
    pub interest_paid: String,
    pub annual_rate: String,
    pub term_months: u32,
    pub capitalization: bool,
    pub product_type: String,
    pub status: String,
    pub opened_at: i64,
    pub maturity_date: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct DepositCoreRepository {
    pub client: Client,
}

impl DepositCoreRepository {
    pub async fn new(config: &YdbConfig) -> YdbResult<Self> {
        let client = create_ydb_client(config).await?;
        Ok(Self { client })
    }

    /// Открытие вклада: списание с текущего счёта + создание счёта вклада
    pub async fn open_deposit(
        &self,
        record: &DepositAccountRecord,
        client_account_id: &str,
        amount: Decimal,
        event_payload: &str,
    ) -> YdbResult<String> {
        let daid = record.deposit_account_id.clone();
        let did = record.deposit_id.clone();
        let cid = record.client_id.clone();
        let ccy = record.currency_code.clone();
        let acc_num = record.account_number.clone();
        let rate_str = record.annual_rate.clone();
        let term = record.term_months as i64;
        let cap = record.capitalization;
        let ptype = record.product_type.clone();
        let opened_at = record.opened_at;
        let maturity = record.maturity_date.clone();
        let caid = client_account_id.to_string();
        let amount_str = amount.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                let did = did.clone();
                let cid = cid.clone();
                let ccy = ccy.clone();
                let acc_num = acc_num.clone();
                let rate_str = rate_str.clone();
                let ptype = ptype.clone();
                let maturity = maturity.clone();
                let caid = caid.clone();
                let amount_str = amount_str.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    // Читаем текущий счёт клиента
                    let ca_res = t
                        .query(
                            Query::from(
                                "SELECT balance, currency_code, status FROM accounts WHERE account_id = $id",
                            )
                                .param("$id", caid.clone()),
                        )
                        .await?;
                    let ca_row = ca_res.into_only_row()?;
                    let ca_balance_str: String = ca_row.get("balance")?.try_into()?;
                    let ca_ccy: String = ca_row.get("currency_code")?.try_into()?;
                    let ca_status: String = ca_row.get("status")?.try_into()?;

                    if ca_status != "active" {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Client account is not active".into(),
                            ..Default::default()
                        }));
                    }
                    if ca_ccy != ccy {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Currency mismatch".into(),
                            ..Default::default()
                        }));
                    }

                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let amount_dec = Decimal::from_str(&amount_str).unwrap();
                    if ca_balance < amount_dec {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Insufficient funds".into(),
                            ..Default::default()
                        }));
                    }

                    let new_ca_balance = ca_balance - amount_dec;

                    // Списание с текущего счёта клиента
                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        )
                            .param("$bal", new_ca_balance.to_string())
                            .param("$id", caid.clone()),
                    )
                        .await?;

                    // Создаём счёт вклада
                    t.query(
                        Query::from(
                            "UPSERT INTO deposit_accounts (deposit_account_id, deposit_id, client_id, currency_code, account_number, \
                             principal_balance, interest_accrued, interest_paid, annual_rate, term_months, capitalization, \
                             product_type, status, opened_at, maturity_date, updated_at) \
                             VALUES ($daid, $did, $cid, $ccy, $acc_num, $amount, 0.00, 0.00, $rate, $term, $cap, \
                             $ptype, 'active', $opened_at, $maturity, $opened_at)",
                        )
                            .param("$daid", daid.clone())
                            .param("$did", did.clone())
                            .param("$cid", cid.clone())
                            .param("$ccy", ccy.clone())
                            .param("$acc_num", acc_num)
                            .param("$amount", amount_str.clone())
                            .param("$rate", rate_str.clone())
                            .param("$term", term)
                            .param("$cap", cap)
                            .param("$ptype", ptype)
                            .param("$opened_at", opened_at)
                            .param("$maturity", maturity),
                    )
                        .await?;

                    // Проводка по счёту клиента (debit)
                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_OPEN', $amount, $ccy, 'completed', 'Открытие вклада')",
                        )
                            .param("$account_id", caid.clone())
                            .param("$created_at", opened_at)
                            .param("$tx_id", tx_id.clone())
                            .param("$amount", amount_str.clone())
                            .param("$ccy", ccy.clone()),
                    )
                        .await?;

                    // Проводка по счёту вклада (credit)
                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_OPEN', $amount, $ccy, 'completed', 'Открытие вклада')",
                        )
                            .param("$account_id", daid.clone())
                            .param("$created_at", opened_at)
                            .param("$tx_id", tx_id.clone())
                            .param("$amount", amount_str.clone())
                            .param("$ccy", ccy.clone()),
                    )
                        .await?;

                    // Outbox
                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositOpened', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id)
                            .param("$deposit_account_id", daid.clone())
                            .param("$payload", payload)
                            .param("$created_at", opened_at),
                    )
                        .await?;

                    Ok(tx_id)
                }
            })
            .await
    }

    /// Пополнение вклада
    pub async fn top_up(
        &self,
        deposit_account_id: &str,
        client_account_id: &str,
        amount: Decimal,
        currency_code: &str,
        event_payload: &str,
    ) -> YdbResult<(String, String)> {
        let daid = deposit_account_id.to_string();
        let caid = client_account_id.to_string();
        let amount_str = amount.to_string();
        let ccy = currency_code.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                let caid = caid.clone();
                let amount_str = amount_str.clone();
                let ccy = ccy.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    // Счёт вклада
                    let da_res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, currency_code, status FROM deposit_accounts WHERE deposit_account_id = $id",
                            )
                                .param("$id", daid.clone()),
                        )
                        .await?;
                    let da_row = da_res.into_only_row()?;
                    let da_balance: String = da_row.get("principal_balance")?.try_into()?;
                    let da_ccy: String = da_row.get("currency_code")?.try_into()?;
                    let da_status: String = da_row.get("status")?.try_into()?;

                    if da_status != "active" {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Deposit is not active".into(),
                            ..Default::default()
                        }));
                    }
                    if da_ccy != ccy {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Currency mismatch".into(),
                            ..Default::default()
                        }));
                    }

                    // Счёт клиента
                    let ca_res = t
                        .query(
                            Query::from(
                                "SELECT balance FROM accounts WHERE account_id = $id",
                            )
                                .param("$id", caid.clone()),
                        )
                        .await?;
                    let ca_balance_str: String =
                        ca_res.into_only_row()?.get("balance")?.try_into()?;

                    let da_current = Decimal::from_str(&da_balance).unwrap();
                    let ca_current = Decimal::from_str(&ca_balance_str).unwrap();
                    let delta = Decimal::from_str(&amount_str).unwrap();

                    if ca_current < delta {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Insufficient funds".into(),
                            ..Default::default()
                        }));
                    }

                    let new_da = da_current + delta;
                    let new_ca = ca_current - delta;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET principal_balance = $bal, updated_at = $now WHERE deposit_account_id = $id",
                        )
                            .param("$bal", new_da.to_string())
                            .param("$now", now)
                            .param("$id", daid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        )
                            .param("$bal", new_ca.to_string())
                            .param("$id", caid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_TOPUP', $amount, $ccy, 'completed', 'Пополнение вклада')",
                        )
                            .param("$account_id", daid.clone())
                            .param("$created_at", now)
                            .param("$tx_id", tx_id.clone())
                            .param("$amount", amount_str.clone())
                            .param("$ccy", ccy.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositToppedUp', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id)
                            .param("$deposit_account_id", daid.clone())
                            .param("$payload", payload)
                            .param("$created_at", now),
                    )
                        .await?;

                    Ok((tx_id, new_da.to_string()))
                }
            })
            .await
    }

    /// Начисление процентов за день
    pub async fn accrue_interest(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        accrual_date: &str,
        annual_rate: Decimal,
        event_payload: &str,
    ) -> YdbResult<(String, String, String)> {
        let daid = deposit_account_id.to_string();
        let did = deposit_id.to_string();
        let adate = accrual_date.to_string();
        let rate_str = annual_rate.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let accrual_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                let did = did.clone();
                let adate = adate.clone();
                let rate_str = rate_str.clone();
                let tx_id = tx_id.clone();
                let accrual_id = accrual_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            )
                                .param("$id", daid.clone()),
                        )
                        .await?;
                    let row = res.into_only_row()?;
                    let principal_str: String = row.get("principal_balance")?.try_into()?;
                    let interest_str: String = row.get("interest_accrued")?.try_into()?;
                    let ccy: String = row.get("currency_code")?.try_into()?;

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let current = Decimal::from_str(&interest_str).unwrap();
                    let rate = Decimal::from_str(&rate_str).unwrap();

                    let daily_rate = rate / Decimal::from(100) / Decimal::from(365);
                    let accrued = (principal * daily_rate).round_dp(2);
                    let new_interest = current + accrued;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET interest_accrued = $i, updated_at = $now WHERE deposit_account_id = $id",
                        )
                            .param("$i", new_interest.to_string())
                            .param("$now", now)
                            .param("$id", daid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO deposit_accruals (deposit_account_id, accrual_date, accrual_id, amount, annual_rate, created_at) \
                             VALUES ($daid, $adate, $aid, $amount, $rate, $now)",
                        )
                            .param("$daid", daid.clone())
                            .param("$adate", adate.clone())
                            .param("$aid", accrual_id)
                            .param("$amount", accrued.to_string())
                            .param("$rate", rate_str.clone())
                            .param("$now", now),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_INTEREST_ACCRUAL', $amount, $ccy, 'completed', 'Начисление процентов по вкладу')",
                        )
                            .param("$account_id", daid.clone())
                            .param("$created_at", now)
                            .param("$tx_id", tx_id.clone())
                            .param("$amount", accrued.to_string())
                            .param("$ccy", ccy),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositInterestAccrued', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id)
                            .param("$deposit_account_id", daid.clone())
                            .param("$payload", payload)
                            .param("$created_at", now),
                    )
                        .await?;

                    Ok((tx_id, accrued.to_string(), new_interest.to_string()))
                }
            })
            .await
    }

    /// Капитализация процентов (причисление к телу)
    pub async fn capitalize_interest(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        event_payload: &str,
    ) -> YdbResult<(String, String, String)> {
        let daid = deposit_account_id.to_string();
        let did = deposit_id.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let cap_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                let did = did.clone();
                let tx_id = tx_id.clone();
                let cap_id = cap_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, capitalization, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            )
                                .param("$id", daid.clone()),
                        )
                        .await?;
                    let row = res.into_only_row()?;
                    let principal_str: String = row.get("principal_balance")?.try_into()?;
                    let interest_str: String = row.get("interest_accrued")?.try_into()?;
                    let cap: bool = row.get("capitalization")?.try_into()?;
                    let ccy: String = row.get("currency_code")?.try_into()?;

                    if !cap {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Deposit doesn't support capitalization".into(),
                            ..Default::default()
                        }));
                    }

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let interest = Decimal::from_str(&interest_str).unwrap();

                    if interest <= Decimal::ZERO {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Nothing to capitalize".into(),
                            ..Default::default()
                        }));
                    }

                    let new_body = principal + interest;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET principal_balance = $p, interest_accrued = 0.00, updated_at = $now WHERE deposit_account_id = $id",
                        )
                            .param("$p", new_body.to_string())
                            .param("$now", now)
                            .param("$id", daid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO deposit_capitalizations (deposit_account_id, created_at, capitalization_id, amount, new_body) \
                             VALUES ($daid, $now, $cid, $amount, $body)",
                        )
                            .param("$daid", daid.clone())
                            .param("$now", now)
                            .param("$cid", cap_id)
                            .param("$amount", interest.to_string())
                            .param("$body", new_body.to_string()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_CAPITALIZATION', $amount, $ccy, 'completed', 'Капитализация процентов')",
                        )
                            .param("$account_id", daid.clone())
                            .param("$created_at", now)
                            .param("$tx_id", tx_id.clone())
                            .param("$amount", interest.to_string())
                            .param("$ccy", ccy),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositInterestCapitalized', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id)
                            .param("$deposit_account_id", daid.clone())
                            .param("$payload", payload)
                            .param("$created_at", now),
                    )
                        .await?;

                    Ok((tx_id, interest.to_string(), new_body.to_string()))
                }
            })
            .await
    }

    /// Выплата процентов на текущий счёт
    pub async fn pay_out_interest(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        client_account_id: &str,
        event_payload: &str,
    ) -> YdbResult<(String, String)> {
        let daid = deposit_account_id.to_string();
        let did = deposit_id.to_string();
        let caid = client_account_id.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                let did = did.clone();
                let caid = caid.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT interest_accrued, interest_paid, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            )
                                .param("$id", daid.clone()),
                        )
                        .await?;
                    let row = res.into_only_row()?;
                    let interest_str: String = row.get("interest_accrued")?.try_into()?;
                    let paid_str: String = row.get("interest_paid")?.try_into()?;
                    let ccy: String = row.get("currency_code")?.try_into()?;

                    let interest = Decimal::from_str(&interest_str).unwrap();
                    let paid = Decimal::from_str(&paid_str).unwrap();

                    if interest <= Decimal::ZERO {
                        return Err(ydb::YdbOrCustomerError::from(ydb::YdbStatusError {
                            message: "Nothing to pay out".into(),
                            ..Default::default()
                        }));
                    }

                    let new_paid = paid + interest;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET interest_accrued = 0.00, interest_paid = $paid, updated_at = $now WHERE deposit_account_id = $id",
                        )
                            .param("$paid", new_paid.to_string())
                            .param("$now", now)
                            .param("$id", daid.clone()),
                    )
                        .await?;

                    // Зачисление на текущий счёт
                    let ca_res = t
                        .query(
                            Query::from("SELECT balance FROM accounts WHERE account_id = $id")
                                .param("$id", caid.clone()),
                        )
                        .await?;
                    let ca_balance_str: String =
                        ca_res.into_only_row()?.get("balance")?.try_into()?;
                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let new_ca = ca_balance + interest;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        )
                            .param("$bal", new_ca.to_string())
                            .param("$id", caid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_INTEREST_PAYOUT', $amount, $ccy, 'completed', 'Выплата процентов по вкладу')",
                        )
                            .param("$account_id", caid.clone())
                            .param("$created_at", now)
                            .param("$tx_id", tx_id.clone())
                            .param("$amount", interest.to_string())
                            .param("$ccy", ccy),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositInterestPaidOut', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id)
                            .param("$deposit_account_id", daid.clone())
                            .param("$payload", payload)
                            .param("$created_at", now),
                    )
                        .await?;

                    Ok((tx_id, interest.to_string()))
                }
            })
            .await
    }

    /// Досрочное расторжение
    pub async fn early_terminate(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        client_account_id: &str,
        early_rate: Decimal,
        termination_date: &str,
        event_payload: &str,
    ) -> YdbResult<(String, String, String)> {
        let daid = deposit_account_id.to_string();
        let did = deposit_id.to_string();
        let caid = client_account_id.to_string();
        let rate_str = early_rate.to_string();
        let term_date = termination_date.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                let did = did.clone();
                let caid = caid.clone();
                let rate_str = rate_str.clone();
                let term_date = term_date.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, opened_at, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            )
                                .param("$id", daid.clone()),
                        )
                        .await?;
                    let row = res.into_only_row()?;
                    let principal_str: String = row.get("principal_balance")?.try_into()?;
                    let _accrued_str: String = row.get("interest_accrued")?.try_into()?;
                    let opened_at: i64 = row.get("opened_at")?.try_into()?;
                    let ccy: String = row.get("currency_code")?.try_into()?;

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let rate = Decimal::from_str(&rate_str).unwrap();

                    // Дни от открытия до расторжения
                    let days_held = (now - opened_at) / 86400;
                    let daily_rate = rate / Decimal::from(100) / Decimal::from(365);
                    let early_interest =
                        (principal * daily_rate * Decimal::from(days_held)).round_dp(2);

                    let total_return = principal + early_interest;

                    // Закрываем вклад
                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET principal_balance = 0.00, interest_accrued = 0.00, status = 'terminated', updated_at = $now WHERE deposit_account_id = $id",
                        )
                            .param("$now", now)
                            .param("$id", daid.clone()),
                    )
                        .await?;

                    // Возврат на текущий счёт клиента
                    let ca_res = t
                        .query(
                            Query::from("SELECT balance FROM accounts WHERE account_id = $id")
                                .param("$id", caid.clone()),
                        )
                        .await?;
                    let ca_balance_str: String =
                        ca_res.into_only_row()?.get("balance")?.try_into()?;
                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let new_ca = ca_balance + total_return;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        )
                            .param("$bal", new_ca.to_string())
                            .param("$id", caid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_EARLY_TERMINATION', $amount, $ccy, 'completed', 'Досрочное расторжение вклада')",
                        )
                            .param("$account_id", caid.clone())
                            .param("$created_at", now)
                            .param("$tx_id", tx_id.clone())
                            .param("$amount", total_return.to_string())
                            .param("$ccy", ccy),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositEarlyTerminated', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id)
                            .param("$deposit_account_id", daid.clone())
                            .param("$payload", payload)
                            .param("$created_at", now),
                    )
                        .await?;

                    Ok((
                        tx_id,
                        principal.to_string(),
                        early_interest.to_string(),
                    ))
                }
            })
            .await
    }

    /// Пролонгация
    pub async fn prolong(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        new_term_months: u32,
        new_annual_rate: Decimal,
        event_payload: &str,
    ) -> YdbResult<String> {
        let daid = deposit_account_id.to_string();
        let did = deposit_id.to_string();
        let rate_str = new_annual_rate.to_string();
        let term = new_term_months as i64;
        let event_id = uuid::Uuid::new_v4().to_string();
        let prolongation_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                let did = did.clone();
                let rate_str = rate_str.clone();
                let event_id = event_id.clone();
                let prolongation_id = prolongation_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT maturity_date FROM deposit_accounts WHERE deposit_account_id = $id",
                            )
                                .param("$id", daid.clone()),
                        )
                        .await?;
                    let old_maturity: String = res
                        .into_only_row()?
                        .get("maturity_date")?
                        .try_into()?;

                    // Новая дата = текущая + term_months
                    let new_maturity_date =
                        (chrono::Utc::now() + chrono::Duration::days(term * 30))
                            .format("%Y-%m-%d")
                            .to_string();

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET maturity_date = $m, annual_rate = $r, term_months = $t, status = 'active', updated_at = $now WHERE deposit_account_id = $id",
                        )
                            .param("$m", new_maturity_date.clone())
                            .param("$r", rate_str.clone())
                            .param("$t", term)
                            .param("$now", now)
                            .param("$id", daid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO deposit_prolongations (deposit_account_id, created_at, prolongation_id, old_maturity_date, new_maturity_date, new_annual_rate, new_term_months) \
                             VALUES ($daid, $now, $pid, $old, $new, $rate, $term)",
                        )
                            .param("$daid", daid.clone())
                            .param("$now", now)
                            .param("$pid", prolongation_id)
                            .param("$old", old_maturity)
                            .param("$new", new_maturity_date.clone())
                            .param("$rate", rate_str.clone())
                            .param("$term", term),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositProlonged', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id)
                            .param("$deposit_account_id", daid.clone())
                            .param("$payload", payload)
                            .param("$created_at", now),
                    )
                        .await?;

                    Ok(new_maturity_date)
                }
            })
            .await
    }

    /// Закрытие вклада
    pub async fn close(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        client_account_id: &str,
        event_payload: &str,
    ) -> YdbResult<(String, String, String)> {
        let daid = deposit_account_id.to_string();
        let did = deposit_id.to_string();
        let caid = client_account_id.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                let did = did.clone();
                let caid = caid.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            )
                                .param("$id", daid.clone()),
                        )
                        .await?;
                    let row = res.into_only_row()?;
                    let principal_str: String = row.get("principal_balance")?.try_into()?;
                    let interest_str: String = row.get("interest_accrued")?.try_into()?;
                    let ccy: String = row.get("currency_code")?.try_into()?;

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let interest = Decimal::from_str(&interest_str).unwrap();
                    let total = principal + interest;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET principal_balance = 0.00, interest_accrued = 0.00, status = 'closed', updated_at = $now WHERE deposit_account_id = $id",
                        )
                            .param("$now", now)
                            .param("$id", daid.clone()),
                    )
                        .await?;

                    let ca_res = t
                        .query(
                            Query::from("SELECT balance FROM accounts WHERE account_id = $id")
                                .param("$id", caid.clone()),
                        )
                        .await?;
                    let ca_balance_str: String =
                        ca_res.into_only_row()?.get("balance")?.try_into()?;
                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let new_ca = ca_balance + total;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        )
                            .param("$bal", new_ca.to_string())
                            .param("$id", caid.clone()),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_CLOSE', $amount, $ccy, 'completed', 'Закрытие вклада')",
                        )
                            .param("$account_id", caid.clone())
                            .param("$created_at", now)
                            .param("$tx_id", tx_id.clone())
                            .param("$amount", total.to_string())
                            .param("$ccy", ccy),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositClosed', $payload, 'PENDING', $created_at, 0)",
                        )
                            .param("$event_id", event_id)
                            .param("$deposit_account_id", daid.clone())
                            .param("$payload", payload)
                            .param("$created_at", now),
                    )
                        .await?;

                    Ok((
                        tx_id,
                        principal.to_string(),
                        interest.to_string(),
                    ))
                }
            })
            .await
    }

    /// Состояние вклада
    pub async fn get_deposit_state(
        &self,
        deposit_account_id: &str,
    ) -> YdbResult<Option<DepositAccountRecord>> {
        let daid = deposit_account_id.to_string();
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| {
                let daid = daid.clone();
                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT deposit_account_id, deposit_id, client_id, currency_code, account_number, \
                                 principal_balance, interest_accrued, interest_paid, annual_rate, term_months, capitalization, \
                                 product_type, status, opened_at, maturity_date, updated_at \
                                 FROM deposit_accounts WHERE deposit_account_id = $id",
                            )
                                .param("$id", daid),
                        )
                        .await?;
                    Ok(res)
                }
            })
            .await?;

        let rows: Vec<_> = result.into_iter().collect();
        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        Ok(Some(DepositAccountRecord {
            deposit_account_id: row.get("deposit_account_id")?.try_into()?,
            deposit_id: row.get("deposit_id")?.try_into()?,
            client_id: row.get("client_id")?.try_into()?,
            currency_code: row.get("currency_code")?.try_into()?,
            account_number: row.get("account_number")?.try_into()?,
            principal_balance: row.get("principal_balance")?.try_into()?,
            interest_accrued: row.get("interest_accrued")?.try_into()?,
            interest_paid: row.get("interest_paid")?.try_into()?,
            annual_rate: row.get("annual_rate")?.try_into()?,
            term_months: {
                let t: i64 = row.get("term_months")?.try_into()?;
                t as u32
            },
            capitalization: row.get("capitalization")?.try_into()?,
            product_type: row.get("product_type")?.try_into()?,
            status: row.get("status")?.try_into()?,
            opened_at: row.get("opened_at")?.try_into()?,
            maturity_date: row.get("maturity_date")?.try_into()?,
            updated_at: row.get("updated_at")?.try_into()?,
        }))
    }
}