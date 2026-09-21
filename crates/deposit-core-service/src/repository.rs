use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use ydb::{ydb_params, Client, Query, YdbError, YdbOrCustomerError};

type RepoResult<T> = Result<T, YdbOrCustomerError>;

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

pub struct DepositCoreRepository {
    pub client: Client,
}

/// Хелпер для бизнес-ошибок. Возвращает `YdbOrCustomerError`,
/// потому что `retry_transaction` ожидает именно этот тип от замыкания.
fn custom_error(msg: impl Into<String>) -> YdbOrCustomerError {
    YdbError::Custom(msg.into()).into()
}

impl DepositCoreRepository {
    pub async fn new(config: &YdbConfig) -> RepoResult<Self> {
        let client = create_ydb_client(config)
            .await
            .map_err(YdbOrCustomerError::from)?;
        Ok(Self { client })
    }

    pub async fn open_deposit(
        &self,
        record: &DepositAccountRecord,
        client_account_id: &str,
        amount: Decimal,
        event_payload: &str,
    ) -> RepoResult<String> {
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
                    let ca_res = t
                        .query(
                            Query::from(
                                "SELECT balance, currency_code, status FROM accounts WHERE account_id = $id",
                            ).with_params(ydb_params!("$id" => caid.clone())),
                        )
                        .await?;
                    let mut ca_row = ca_res.into_only_row()?;
                    let ca_balance_str: String =
                        ca_row.remove_field_by_name("balance")?.try_into()?;
                    let ca_ccy: String =
                        ca_row.remove_field_by_name("currency_code")?.try_into()?;
                    let ca_status: String =
                        ca_row.remove_field_by_name("status")?.try_into()?;

                    if ca_status != "active" {
                        return Err(custom_error("Client account is not active"));
                    }
                    if ca_ccy != ccy {
                        return Err(custom_error("Currency mismatch"));
                    }

                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let amount_dec = Decimal::from_str(&amount_str).unwrap();
                    if ca_balance < amount_dec {
                        return Err(custom_error("Insufficient funds"));
                    }

                    let new_ca_balance = ca_balance - amount_dec;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        ).with_params(ydb_params!(
                            "$bal" => new_ca_balance.to_string(),
                            "$id" => caid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO deposit_accounts (deposit_account_id, deposit_id, client_id, currency_code, account_number, \
                             principal_balance, interest_accrued, interest_paid, annual_rate, term_months, capitalization, \
                             product_type, status, opened_at, maturity_date, updated_at) \
                             VALUES ($daid, $did, $cid, $ccy, $acc_num, $amount, 0.00, 0.00, $rate, $term, $cap, \
                             $ptype, 'active', $opened_at, $maturity, $opened_at)",
                        ).with_params(ydb_params!(
                            "$daid" => daid.clone(),
                            "$did" => did.clone(),
                            "$cid" => cid.clone(),
                            "$ccy" => ccy.clone(),
                            "$acc_num" => acc_num,
                            "$amount" => amount_str.clone(),
                            "$rate" => rate_str.clone(),
                            "$term" => term,
                            "$cap" => cap,
                            "$ptype" => ptype,
                            "$opened_at" => opened_at,
                            "$maturity" => maturity
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_OPEN', $amount, $ccy, 'completed', 'Открытие вклада')",
                        ).with_params(ydb_params!(
                            "$account_id" => caid.clone(),
                            "$created_at" => opened_at,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => amount_str.clone(),
                            "$ccy" => ccy.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_OPEN', $amount, $ccy, 'completed', 'Открытие вклада')",
                        ).with_params(ydb_params!(
                            "$account_id" => daid.clone(),
                            "$created_at" => opened_at,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => amount_str.clone(),
                            "$ccy" => ccy.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositOpened', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$deposit_account_id" => daid.clone(),
                            "$payload" => payload,
                            "$created_at" => opened_at
                        )),
                    )
                        .await?;

                    Ok(tx_id)
                }
            })
            .await
    }

    pub async fn top_up(
        &self,
        deposit_account_id: &str,
        client_account_id: &str,
        amount: Decimal,
        currency_code: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String)> {
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
                    let da_res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, currency_code, status FROM deposit_accounts WHERE deposit_account_id = $id",
                            ).with_params(ydb_params!("$id" => daid.clone())),
                        )
                        .await?;
                    let mut da_row = da_res.into_only_row()?;
                    let da_balance: String =
                        da_row.remove_field_by_name("principal_balance")?.try_into()?;
                    let da_ccy: String =
                        da_row.remove_field_by_name("currency_code")?.try_into()?;
                    let da_status: String =
                        da_row.remove_field_by_name("status")?.try_into()?;

                    if da_status != "active" {
                        return Err(custom_error("Deposit is not active"));
                    }
                    if da_ccy != ccy {
                        return Err(custom_error("Currency mismatch"));
                    }

                    let ca_res = t
                        .query(
                            Query::from("SELECT balance FROM accounts WHERE account_id = $id")
                                .with_params(ydb_params!("$id" => caid.clone())),
                        )
                        .await?;
                    let ca_balance_str: String = ca_res
                        .into_only_row()?
                        .remove_field_by_name("balance")?
                        .try_into()?;

                    let da_current = Decimal::from_str(&da_balance).unwrap();
                    let ca_current = Decimal::from_str(&ca_balance_str).unwrap();
                    let delta = Decimal::from_str(&amount_str).unwrap();

                    if ca_current < delta {
                        return Err(custom_error("Insufficient funds"));
                    }

                    let new_da = da_current + delta;
                    let new_ca = ca_current - delta;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET principal_balance = $bal, updated_at = $now WHERE deposit_account_id = $id",
                        ).with_params(ydb_params!(
                            "$bal" => new_da.to_string(),
                            "$now" => now,
                            "$id" => daid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        ).with_params(ydb_params!(
                            "$bal" => new_ca.to_string(),
                            "$id" => caid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_TOPUP', $amount, $ccy, 'completed', 'Пополнение вклада')",
                        ).with_params(ydb_params!(
                            "$account_id" => daid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => amount_str.clone(),
                            "$ccy" => ccy.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositToppedUp', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$deposit_account_id" => daid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok((tx_id, new_da.to_string()))
                }
            })
            .await
    }

    pub async fn accrue_interest(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        accrual_date: &str,
        annual_rate: Decimal,
        event_payload: &str,
    ) -> RepoResult<(String, String, String)> {
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
                    let _ = &did;
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            ).with_params(ydb_params!("$id" => daid.clone())),
                        )
                        .await?;
                    let mut row = res.into_only_row()?;
                    let principal_str: String =
                        row.remove_field_by_name("principal_balance")?.try_into()?;
                    let interest_str: String =
                        row.remove_field_by_name("interest_accrued")?.try_into()?;
                    let ccy: String =
                        row.remove_field_by_name("currency_code")?.try_into()?;

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let current = Decimal::from_str(&interest_str).unwrap();
                    let rate = Decimal::from_str(&rate_str).unwrap();

                    let daily_rate = rate / Decimal::from(100) / Decimal::from(365);
                    let accrued = (principal * daily_rate).round_dp(2);
                    let new_interest = current + accrued;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET interest_accrued = $i, updated_at = $now WHERE deposit_account_id = $id",
                        ).with_params(ydb_params!(
                            "$i" => new_interest.to_string(),
                            "$now" => now,
                            "$id" => daid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO deposit_accruals (deposit_account_id, accrual_date, accrual_id, amount, annual_rate, created_at) \
                             VALUES ($daid, $adate, $aid, $amount, $rate, $now)",
                        ).with_params(ydb_params!(
                            "$daid" => daid.clone(),
                            "$adate" => adate.clone(),
                            "$aid" => accrual_id,
                            "$amount" => accrued.to_string(),
                            "$rate" => rate_str.clone(),
                            "$now" => now
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_INTEREST_ACCRUAL', $amount, $ccy, 'completed', 'Начисление процентов по вкладу')",
                        ).with_params(ydb_params!(
                            "$account_id" => daid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => accrued.to_string(),
                            "$ccy" => ccy
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositInterestAccrued', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$deposit_account_id" => daid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok((tx_id, accrued.to_string(), new_interest.to_string()))
                }
            })
            .await
    }

    pub async fn capitalize_interest(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String, String)> {
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
                    let _ = &did;
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, capitalization, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            ).with_params(ydb_params!("$id" => daid.clone())),
                        )
                        .await?;
                    let mut row = res.into_only_row()?;
                    let principal_str: String =
                        row.remove_field_by_name("principal_balance")?.try_into()?;
                    let interest_str: String =
                        row.remove_field_by_name("interest_accrued")?.try_into()?;
                    let cap: bool =
                        row.remove_field_by_name("capitalization")?.try_into()?;
                    let ccy: String =
                        row.remove_field_by_name("currency_code")?.try_into()?;

                    if !cap {
                        return Err(custom_error(
                            "Deposit doesn't support capitalization",
                        ));
                    }

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let interest = Decimal::from_str(&interest_str).unwrap();

                    if interest <= Decimal::ZERO {
                        return Err(custom_error("Nothing to capitalize"));
                    }

                    let new_body = principal + interest;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET principal_balance = $p, interest_accrued = 0.00, updated_at = $now WHERE deposit_account_id = $id",
                        ).with_params(ydb_params!(
                            "$p" => new_body.to_string(),
                            "$now" => now,
                            "$id" => daid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO deposit_capitalizations (deposit_account_id, created_at, capitalization_id, amount, new_body) \
                             VALUES ($daid, $now, $cid, $amount, $body)",
                        ).with_params(ydb_params!(
                            "$daid" => daid.clone(),
                            "$now" => now,
                            "$cid" => cap_id,
                            "$amount" => interest.to_string(),
                            "$body" => new_body.to_string()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_CAPITALIZATION', $amount, $ccy, 'completed', 'Капитализация процентов')",
                        ).with_params(ydb_params!(
                            "$account_id" => daid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => interest.to_string(),
                            "$ccy" => ccy
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositInterestCapitalized', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$deposit_account_id" => daid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok((tx_id, interest.to_string(), new_body.to_string()))
                }
            })
            .await
    }

    pub async fn pay_out_interest(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        client_account_id: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String)> {
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
                    let _ = &did;
                    let res = t
                        .query(
                            Query::from(
                                "SELECT interest_accrued, interest_paid, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            ).with_params(ydb_params!("$id" => daid.clone())),
                        )
                        .await?;
                    let mut row = res.into_only_row()?;
                    let interest_str: String =
                        row.remove_field_by_name("interest_accrued")?.try_into()?;
                    let paid_str: String =
                        row.remove_field_by_name("interest_paid")?.try_into()?;
                    let ccy: String =
                        row.remove_field_by_name("currency_code")?.try_into()?;

                    let interest = Decimal::from_str(&interest_str).unwrap();
                    let paid = Decimal::from_str(&paid_str).unwrap();

                    if interest <= Decimal::ZERO {
                        return Err(custom_error("Nothing to pay out"));
                    }

                    let new_paid = paid + interest;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET interest_accrued = 0.00, interest_paid = $paid, updated_at = $now WHERE deposit_account_id = $id",
                        ).with_params(ydb_params!(
                            "$paid" => new_paid.to_string(),
                            "$now" => now,
                            "$id" => daid.clone()
                        )),
                    )
                        .await?;

                    let ca_res = t
                        .query(
                            Query::from("SELECT balance FROM accounts WHERE account_id = $id")
                                .with_params(ydb_params!("$id" => caid.clone())),
                        )
                        .await?;
                    let ca_balance_str: String = ca_res
                        .into_only_row()?
                        .remove_field_by_name("balance")?
                        .try_into()?;
                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let new_ca = ca_balance + interest;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        ).with_params(ydb_params!(
                            "$bal" => new_ca.to_string(),
                            "$id" => caid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_INTEREST_PAYOUT', $amount, $ccy, 'completed', 'Выплата процентов по вкладу')",
                        ).with_params(ydb_params!(
                            "$account_id" => caid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => interest.to_string(),
                            "$ccy" => ccy
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositInterestPaidOut', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$deposit_account_id" => daid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok((tx_id, interest.to_string()))
                }
            })
            .await
    }

    pub async fn early_terminate(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        client_account_id: &str,
        early_rate: Decimal,
        termination_date: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String, String)> {
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
                    let _ = (&did, &term_date);
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, opened_at, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            ).with_params(ydb_params!("$id" => daid.clone())),
                        )
                        .await?;
                    let mut row = res.into_only_row()?;
                    let principal_str: String =
                        row.remove_field_by_name("principal_balance")?.try_into()?;
                    let _accrued_str: String =
                        row.remove_field_by_name("interest_accrued")?.try_into()?;
                    let opened_at: i64 =
                        row.remove_field_by_name("opened_at")?.try_into()?;
                    let ccy: String =
                        row.remove_field_by_name("currency_code")?.try_into()?;

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let rate = Decimal::from_str(&rate_str).unwrap();

                    let days_held = (now - opened_at) / 86400;
                    let daily_rate = rate / Decimal::from(100) / Decimal::from(365);
                    let early_interest =
                        (principal * daily_rate * Decimal::from(days_held)).round_dp(2);

                    let total_return = principal + early_interest;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET principal_balance = 0.00, interest_accrued = 0.00, status = 'terminated', updated_at = $now WHERE deposit_account_id = $id",
                        ).with_params(ydb_params!(
                            "$now" => now,
                            "$id" => daid.clone()
                        )),
                    )
                        .await?;

                    let ca_res = t
                        .query(
                            Query::from("SELECT balance FROM accounts WHERE account_id = $id")
                                .with_params(ydb_params!("$id" => caid.clone())),
                        )
                        .await?;
                    let ca_balance_str: String = ca_res
                        .into_only_row()?
                        .remove_field_by_name("balance")?
                        .try_into()?;
                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let new_ca = ca_balance + total_return;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        ).with_params(ydb_params!(
                            "$bal" => new_ca.to_string(),
                            "$id" => caid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_EARLY_TERMINATION', $amount, $ccy, 'completed', 'Досрочное расторжение вклада')",
                        ).with_params(ydb_params!(
                            "$account_id" => caid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => total_return.to_string(),
                            "$ccy" => ccy
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositEarlyTerminated', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$deposit_account_id" => daid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
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

    pub async fn prolong(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        new_term_months: u32,
        new_annual_rate: Decimal,
        event_payload: &str,
    ) -> RepoResult<String> {
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
                    let _ = &did;
                    let res = t
                        .query(
                            Query::from(
                                "SELECT maturity_date FROM deposit_accounts WHERE deposit_account_id = $id",
                            ).with_params(ydb_params!("$id" => daid.clone())),
                        )
                        .await?;
                    let old_maturity: String = res
                        .into_only_row()?
                        .remove_field_by_name("maturity_date")?
                        .try_into()?;

                    let new_maturity_date =
                        (chrono::Utc::now() + chrono::Duration::days(term * 30))
                            .format("%Y-%m-%d")
                            .to_string();

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET maturity_date = $m, annual_rate = $r, term_months = $t, status = 'active', updated_at = $now WHERE deposit_account_id = $id",
                        ).with_params(ydb_params!(
                            "$m" => new_maturity_date.clone(),
                            "$r" => rate_str.clone(),
                            "$t" => term,
                            "$now" => now,
                            "$id" => daid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO deposit_prolongations (deposit_account_id, created_at, prolongation_id, old_maturity_date, new_maturity_date, new_annual_rate, new_term_months) \
                             VALUES ($daid, $now, $pid, $old, $new, $rate, $term)",
                        ).with_params(ydb_params!(
                            "$daid" => daid.clone(),
                            "$now" => now,
                            "$pid" => prolongation_id,
                            "$old" => old_maturity,
                            "$new" => new_maturity_date.clone(),
                            "$rate" => rate_str.clone(),
                            "$term" => term
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositProlonged', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$deposit_account_id" => daid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok(new_maturity_date)
                }
            })
            .await
    }

    pub async fn close(
        &self,
        deposit_account_id: &str,
        deposit_id: &str,
        client_account_id: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String, String)> {
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
                    let _ = &did;
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, currency_code FROM deposit_accounts WHERE deposit_account_id = $id",
                            ).with_params(ydb_params!("$id" => daid.clone())),
                        )
                        .await?;
                    let mut row = res.into_only_row()?;
                    let principal_str: String =
                        row.remove_field_by_name("principal_balance")?.try_into()?;
                    let interest_str: String =
                        row.remove_field_by_name("interest_accrued")?.try_into()?;
                    let ccy: String =
                        row.remove_field_by_name("currency_code")?.try_into()?;

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let interest = Decimal::from_str(&interest_str).unwrap();
                    let total = principal + interest;

                    t.query(
                        Query::from(
                            "UPDATE deposit_accounts SET principal_balance = 0.00, interest_accrued = 0.00, status = 'closed', updated_at = $now WHERE deposit_account_id = $id",
                        ).with_params(ydb_params!(
                            "$now" => now,
                            "$id" => daid.clone()
                        )),
                    )
                        .await?;

                    let ca_res = t
                        .query(
                            Query::from("SELECT balance FROM accounts WHERE account_id = $id")
                                .with_params(ydb_params!("$id" => caid.clone())),
                        )
                        .await?;
                    let ca_balance_str: String = ca_res
                        .into_only_row()?
                        .remove_field_by_name("balance")?
                        .try_into()?;
                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let new_ca = ca_balance + total;

                    t.query(
                        Query::from(
                            "UPDATE accounts SET balance = $bal WHERE account_id = $id",
                        ).with_params(ydb_params!(
                            "$bal" => new_ca.to_string(),
                            "$id" => caid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'DEPOSIT_CLOSE', $amount, $ccy, 'completed', 'Закрытие вклада')",
                        ).with_params(ydb_params!(
                            "$account_id" => caid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => total.to_string(),
                            "$ccy" => ccy
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'deposit_account', $deposit_account_id, 'DepositClosed', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$deposit_account_id" => daid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
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

    pub async fn get_deposit_state(
        &self,
        deposit_account_id: &str,
    ) -> RepoResult<Option<DepositAccountRecord>> {
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
                            ).with_params(ydb_params!("$id" => daid)),
                        )
                        .await?;
                    Ok(res)
                }
            })
            .await?;

        let rows: Vec<_> = result.into_only_result()?.rows().collect();
        if rows.is_empty() {
            return Ok(None);
        }
        let mut row = rows.into_iter().next().unwrap();

        Ok(Some(DepositAccountRecord {
            deposit_account_id: row.remove_field_by_name("deposit_account_id")?.try_into()?,
            deposit_id: row.remove_field_by_name("deposit_id")?.try_into()?,
            client_id: row.remove_field_by_name("client_id")?.try_into()?,
            currency_code: row.remove_field_by_name("currency_code")?.try_into()?,
            account_number: row.remove_field_by_name("account_number")?.try_into()?,
            principal_balance: row.remove_field_by_name("principal_balance")?.try_into()?,
            interest_accrued: row.remove_field_by_name("interest_accrued")?.try_into()?,
            interest_paid: row.remove_field_by_name("interest_paid")?.try_into()?,
            annual_rate: row.remove_field_by_name("annual_rate")?.try_into()?,
            term_months: {
                let t: i64 = row.remove_field_by_name("term_months")?.try_into()?;
                t as u32
            },
            capitalization: row.remove_field_by_name("capitalization")?.try_into()?,
            product_type: row.remove_field_by_name("product_type")?.try_into()?,
            status: row.remove_field_by_name("status")?.try_into()?,
            opened_at: row.remove_field_by_name("opened_at")?.try_into()?,
            maturity_date: row.remove_field_by_name("maturity_date")?.try_into()?,
            updated_at: row.remove_field_by_name("updated_at")?.try_into()?,
        }))
    }
}