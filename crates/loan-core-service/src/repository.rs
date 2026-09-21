use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use ydb::{ydb_params, Client, Query, YdbError, YdbOrCustomerError};

type RepoResult<T> = Result<T, YdbOrCustomerError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoanAccountRecord {
    pub loan_account_id: String,
    pub loan_id: String,
    pub client_id: String,
    pub currency_code: String,
    pub account_number: String,
    pub principal_balance: String,
    pub interest_accrued: String,
    pub interest_overdue: String,
    pub principal_overdue: String,
    pub penalty_accrued: String,
    pub provision_amount: String,
    pub product_type: String,
    pub status: String,
    pub opened_at: i64,
    pub updated_at: i64,
}

pub struct LoanCoreRepository {
    pub client: Client,
}

fn custom_error(msg: impl Into<String>) -> YdbError {
    YdbError::Custom(msg.into())
}

impl LoanCoreRepository {
    pub async fn new(config: &YdbConfig) -> RepoResult<Self> {
        let client = create_ydb_client(config)
            .await
            .map_err(YdbOrCustomerError::from)?;
        Ok(Self { client })
    }

    pub async fn open_loan_account(
        &self,
        record: &LoanAccountRecord,
        event_payload: &str,
    ) -> RepoResult<()> {
        let loan_account_id = record.loan_account_id.clone();
        let loan_id = record.loan_id.clone();
        let client_id = record.client_id.clone();
        let currency_code = record.currency_code.clone();
        let account_number = record.account_number.clone();
        let product_type = record.product_type.clone();
        let status = record.status.clone();
        let opened_at = record.opened_at;
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let loan_account_id = loan_account_id.clone();
                let loan_id = loan_id.clone();
                let client_id = client_id.clone();
                let currency_code = currency_code.clone();
                let account_number = account_number.clone();
                let product_type = product_type.clone();
                let status = status.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    t.query(
                        Query::from(
                            "UPSERT INTO loan_accounts (loan_account_id, loan_id, client_id, currency_code, account_number, \
                             principal_balance, interest_accrued, interest_overdue, principal_overdue, penalty_accrued, \
                             provision_amount, product_type, status, opened_at, updated_at) \
                             VALUES ($id, $loan_id, $client_id, $currency_code, $acc_num, \
                             0.00, 0.00, 0.00, 0.00, 0.00, 0.00, $product_type, $status, $opened_at, $opened_at)",
                        ).with_params(ydb_params!(
                            "$id" => loan_account_id.clone(),
                            "$loan_id" => loan_id.clone(),
                            "$client_id" => client_id.clone(),
                            "$currency_code" => currency_code,
                            "$acc_num" => account_number,
                            "$product_type" => product_type,
                            "$status" => status,
                            "$opened_at" => opened_at
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'loan_account', $loan_account_id, 'LoanAccountOpened', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$loan_account_id" => loan_account_id,
                            "$payload" => payload,
                            "$created_at" => opened_at
                        )),
                    )
                        .await?;

                    Ok(())
                }
            })
            .await
    }

    pub async fn disburse_loan(
        &self,
        loan_account_id: &str,
        client_account_id: &str,
        amount: Decimal,
        currency_code: &str,
        loan_id: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String)> {
        let laid = loan_account_id.to_string();
        let caid = client_account_id.to_string();
        let amount_str = amount.to_string();
        let ccy = currency_code.to_string();
        let lid = loan_id.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let laid = laid.clone();
                let caid = caid.clone();
                let amount_str = amount_str.clone();
                let ccy = ccy.clone();
                let lid = lid.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let _ = &lid;

                    let la_res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, currency_code, status FROM loan_accounts WHERE loan_account_id = $id",
                            ).with_params(ydb_params!("$id" => laid.clone())),
                        )
                        .await?;
                    let la_row = la_res.into_only_row()?;
                    let la_balance: String =
                        la_row.remove_field_by_name("principal_balance")?.try_into()?;
                    let la_ccy: String =
                        la_row.remove_field_by_name("currency_code")?.try_into()?;
                    let la_status: String =
                        la_row.remove_field_by_name("status")?.try_into()?;

                    if la_status == "closed" {
                        return Err(custom_error("Loan account is closed"));
                    }
                    if la_ccy != ccy {
                        return Err(custom_error("Currency mismatch on loan account"));
                    }

                    let ca_res = t
                        .query(
                            Query::from(
                                "SELECT balance, currency_code, status FROM accounts WHERE account_id = $id",
                            ).with_params(ydb_params!("$id" => caid.clone())),
                        )
                        .await?;
                    let ca_row = ca_res.into_only_row()?;
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
                        return Err(custom_error("Currency mismatch on client account"));
                    }

                    let la_current = Decimal::from_str(&la_balance).unwrap();
                    let ca_current = Decimal::from_str(&ca_balance_str).unwrap();
                    let delta = Decimal::from_str(&amount_str).unwrap();

                    let new_la_balance = la_current + delta;
                    let new_ca_balance = ca_current + delta;

                    t.query(
                        Query::from(
                            "UPDATE loan_accounts SET principal_balance = $bal, updated_at = $now WHERE loan_account_id = $id",
                        ).with_params(ydb_params!(
                            "$bal" => new_la_balance.to_string(),
                            "$now" => now,
                            "$id" => laid.clone()
                        )),
                    )
                        .await?;

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
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'LOAN_DISBURSEMENT', $amount, $ccy, 'completed', 'Выдача кредита')",
                        ).with_params(ydb_params!(
                            "$account_id" => laid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => amount_str.clone(),
                            "$ccy" => ccy.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'LOAN_DISBURSEMENT', $amount, $ccy, 'completed', 'Выдача кредита')",
                        ).with_params(ydb_params!(
                            "$account_id" => caid.clone(),
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
                             VALUES ($event_id, 'loan_account', $loan_account_id, 'LoanDisbursed', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$loan_account_id" => laid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok((tx_id, new_la_balance.to_string()))
                }
            })
            .await
    }

    pub async fn accrue_interest(
        &self,
        loan_account_id: &str,
        loan_id: &str,
        accrual_date: &str,
        annual_rate: Decimal,
        event_payload: &str,
    ) -> RepoResult<(String, String, String)> {
        let laid = loan_account_id.to_string();
        let lid = loan_id.to_string();
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
                let laid = laid.clone();
                let lid = lid.clone();
                let adate = adate.clone();
                let rate_str = rate_str.clone();
                let tx_id = tx_id.clone();
                let accrual_id = accrual_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let _ = &lid;

                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, currency_code FROM loan_accounts WHERE loan_account_id = $id",
                            ).with_params(ydb_params!("$id" => laid.clone())),
                        )
                        .await?;
                    let row = res.into_only_row()?;
                    let principal_str: String =
                        row.remove_field_by_name("principal_balance")?.try_into()?;
                    let interest_str: String =
                        row.remove_field_by_name("interest_accrued")?.try_into()?;
                    let ccy: String =
                        row.remove_field_by_name("currency_code")?.try_into()?;

                    let principal = Decimal::from_str(&principal_str).unwrap();
                    let current_interest = Decimal::from_str(&interest_str).unwrap();
                    let rate = Decimal::from_str(&rate_str).unwrap();

                    let daily_rate = rate / Decimal::from(100) / Decimal::from(365);
                    let accrued = (principal * daily_rate).round_dp(2);

                    let new_interest = current_interest + accrued;

                    t.query(
                        Query::from(
                            "UPDATE loan_accounts SET interest_accrued = $interest, updated_at = $now WHERE loan_account_id = $id",
                        ).with_params(ydb_params!(
                            "$interest" => new_interest.to_string(),
                            "$now" => now,
                            "$id" => laid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO interest_accruals (loan_account_id, accrual_date, accrual_id, amount, annual_rate, created_at) \
                             VALUES ($laid, $adate, $aid, $amount, $rate, $now)",
                        ).with_params(ydb_params!(
                            "$laid" => laid.clone(),
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
                             VALUES ($account_id, $created_at, $tx_id, 'INTEREST_ACCRUAL', $amount, $ccy, 'completed', 'Начисление процентов')",
                        ).with_params(ydb_params!(
                            "$account_id" => laid.clone(),
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
                             VALUES ($event_id, 'loan_account', $loan_account_id, 'InterestAccrued', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$loan_account_id" => laid.clone(),
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

    pub async fn repay_loan(
        &self,
        loan_account_id: &str,
        client_account_id: &str,
        principal: Decimal,
        interest: Decimal,
        penalty: Decimal,
        currency_code: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String, String)> {
        let laid = loan_account_id.to_string();
        let caid = client_account_id.to_string();
        let principal_str = principal.to_string();
        let interest_str = interest.to_string();
        let penalty_str = penalty.to_string();
        let ccy = currency_code.to_string();
        let total = principal + interest + penalty;
        let total_str = total.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let laid = laid.clone();
                let caid = caid.clone();
                let principal_str = principal_str.clone();
                let interest_str = interest_str.clone();
                let penalty_str = penalty_str.clone();
                let ccy = ccy.clone();
                let total_str = total_str.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let la_res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, interest_accrued, penalty_accrued, currency_code FROM loan_accounts WHERE loan_account_id = $id",
                            ).with_params(ydb_params!("$id" => laid.clone())),
                        )
                        .await?;
                    let la_row = la_res.into_only_row()?;
                    let la_principal: String =
                        la_row.remove_field_by_name("principal_balance")?.try_into()?;
                    let la_interest: String =
                        la_row.remove_field_by_name("interest_accrued")?.try_into()?;
                    let la_penalty: String =
                        la_row.remove_field_by_name("penalty_accrued")?.try_into()?;
                    let la_ccy: String =
                        la_row.remove_field_by_name("currency_code")?.try_into()?;

                    if la_ccy != ccy {
                        return Err(custom_error("Currency mismatch"));
                    }

                    let new_principal = Decimal::from_str(&la_principal).unwrap()
                        - Decimal::from_str(&principal_str).unwrap();
                    let new_interest = Decimal::from_str(&la_interest).unwrap()
                        - Decimal::from_str(&interest_str).unwrap();
                    let new_penalty = Decimal::from_str(&la_penalty).unwrap()
                        - Decimal::from_str(&penalty_str).unwrap();

                    if new_principal < Decimal::ZERO {
                        return Err(custom_error("Overpayment of principal"));
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
                    let ca_balance = Decimal::from_str(&ca_balance_str).unwrap();
                    let new_ca_balance = ca_balance - Decimal::from_str(&total_str).unwrap();

                    t.query(
                        Query::from(
                            "UPDATE loan_accounts SET principal_balance = $p, interest_accrued = $i, penalty_accrued = $pen, updated_at = $now WHERE loan_account_id = $id",
                        ).with_params(ydb_params!(
                            "$p" => new_principal.to_string(),
                            "$i" => new_interest.to_string(),
                            "$pen" => new_penalty.to_string(),
                            "$now" => now,
                            "$id" => laid.clone()
                        )),
                    )
                        .await?;

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
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'LOAN_REPAYMENT', $amount, $ccy, 'completed', 'Погашение кредита')",
                        ).with_params(ydb_params!(
                            "$account_id" => laid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => total_str.clone(),
                            "$ccy" => ccy.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO transactions (account_id, created_at, transaction_id, operation_code, amount, currency_code, status, description) \
                             VALUES ($account_id, $created_at, $tx_id, 'LOAN_REPAYMENT', $amount, $ccy, 'completed', 'Погашение кредита')",
                        ).with_params(ydb_params!(
                            "$account_id" => caid.clone(),
                            "$created_at" => now,
                            "$tx_id" => tx_id.clone(),
                            "$amount" => total_str.clone(),
                            "$ccy" => ccy
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'loan_account', $loan_account_id, 'LoanRepaid', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$loan_account_id" => laid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok((
                        tx_id,
                        new_principal.to_string(),
                        new_interest.to_string(),
                    ))
                }
            })
            .await
    }

    pub async fn mark_overdue(
        &self,
        loan_account_id: &str,
        principal_overdue: Decimal,
        interest_overdue: Decimal,
        event_payload: &str,
    ) -> RepoResult<String> {
        let laid = loan_account_id.to_string();
        let p_str = principal_overdue.to_string();
        let i_str = interest_overdue.to_string();
        let tx_id = uuid::Uuid::new_v4().to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let laid = laid.clone();
                let p_str = p_str.clone();
                let i_str = i_str.clone();
                let tx_id = tx_id.clone();
                let event_id = event_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_overdue, interest_overdue FROM loan_accounts WHERE loan_account_id = $id",
                            ).with_params(ydb_params!("$id" => laid.clone())),
                        )
                        .await?;
                    let row = res.into_only_row()?;
                    let po: String =
                        row.remove_field_by_name("principal_overdue")?.try_into()?;
                    let io: String =
                        row.remove_field_by_name("interest_overdue")?.try_into()?;

                    let new_po =
                        Decimal::from_str(&po).unwrap() + Decimal::from_str(&p_str).unwrap();
                    let new_io =
                        Decimal::from_str(&io).unwrap() + Decimal::from_str(&i_str).unwrap();

                    t.query(
                        Query::from(
                            "UPDATE loan_accounts SET principal_overdue = $po, interest_overdue = $io, status = 'overdue', updated_at = $now WHERE loan_account_id = $id",
                        ).with_params(ydb_params!(
                            "$po" => new_po.to_string(),
                            "$io" => new_io.to_string(),
                            "$now" => now,
                            "$id" => laid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'loan_account', $loan_account_id, 'LoanMarkedOverdue', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$loan_account_id" => laid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok(tx_id)
                }
            })
            .await
    }

    pub async fn calculate_provision(
        &self,
        loan_account_id: &str,
        category: &str,
        event_payload: &str,
    ) -> RepoResult<(String, String)> {
        let laid = loan_account_id.to_string();
        let cat = category.to_string();
        let event_id = uuid::Uuid::new_v4().to_string();
        let provision_id = uuid::Uuid::new_v4().to_string();
        let payload = event_payload.to_string();
        let now = chrono::Utc::now().timestamp();

        self.client
            .table_client()
            .retry_transaction(|mut t| {
                let laid = laid.clone();
                let cat = cat.clone();
                let event_id = event_id.clone();
                let provision_id = provision_id.clone();
                let payload = payload.clone();

                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT principal_balance, principal_overdue, interest_overdue FROM loan_accounts WHERE loan_account_id = $id",
                            ).with_params(ydb_params!("$id" => laid.clone())),
                        )
                        .await?;
                    let row = res.into_only_row()?;
                    let principal: String =
                        row.remove_field_by_name("principal_balance")?.try_into()?;
                    let po: String =
                        row.remove_field_by_name("principal_overdue")?.try_into()?;
                    let io: String =
                        row.remove_field_by_name("interest_overdue")?.try_into()?;

                    let total = Decimal::from_str(&principal).unwrap()
                        + Decimal::from_str(&po).unwrap()
                        + Decimal::from_str(&io).unwrap();

                    let coeff = match cat.as_str() {
                        "standard" => Decimal::from_str("0.0").unwrap(),
                        "substandard" => Decimal::from_str("0.10").unwrap(),
                        "doubtful" => Decimal::from_str("0.50").unwrap(),
                        "bad" => Decimal::from_str("1.0").unwrap(),
                        _ => Decimal::from_str("0.0").unwrap(),
                    };

                    let provision = (total * coeff).round_dp(2);

                    t.query(
                        Query::from(
                            "UPDATE loan_accounts SET provision_amount = $p, updated_at = $now WHERE loan_account_id = $id",
                        ).with_params(ydb_params!(
                            "$p" => provision.to_string(),
                            "$now" => now,
                            "$id" => laid.clone()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO provisions (loan_account_id, created_at, provision_id, category, amount) \
                             VALUES ($laid, $now, $pid, $cat, $amount)",
                        ).with_params(ydb_params!(
                            "$laid" => laid.clone(),
                            "$now" => now,
                            "$pid" => provision_id,
                            "$cat" => cat.clone(),
                            "$amount" => provision.to_string()
                        )),
                    )
                        .await?;

                    t.query(
                        Query::from(
                            "UPSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, status, created_at, retry_count) \
                             VALUES ($event_id, 'loan_account', $loan_account_id, 'ProvisionCalculated', $payload, 'PENDING', $created_at, 0)",
                        ).with_params(ydb_params!(
                            "$event_id" => event_id,
                            "$loan_account_id" => laid.clone(),
                            "$payload" => payload,
                            "$created_at" => now
                        )),
                    )
                        .await?;

                    Ok((provision.to_string(), cat))
                }
            })
            .await
    }

    pub async fn get_loan_account_state(
        &self,
        loan_account_id: &str,
    ) -> RepoResult<Option<LoanAccountRecord>> {
        let laid = loan_account_id.to_string();
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| {
                let laid = laid.clone();
                async move {
                    let res = t
                        .query(
                            Query::from(
                                "SELECT loan_account_id, loan_id, client_id, currency_code, account_number, \
                                 principal_balance, interest_accrued, interest_overdue, principal_overdue, penalty_accrued, \
                                 provision_amount, product_type, status, opened_at, updated_at \
                                 FROM loan_accounts WHERE loan_account_id = $id",
                            ).with_params(ydb_params!("$id" => laid)),
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

        Ok(Some(LoanAccountRecord {
            loan_account_id: row.remove_field_by_name("loan_account_id")?.try_into()?,
            loan_id: row.remove_field_by_name("loan_id")?.try_into()?,
            client_id: row.remove_field_by_name("client_id")?.try_into()?,
            currency_code: row.remove_field_by_name("currency_code")?.try_into()?,
            account_number: row.remove_field_by_name("account_number")?.try_into()?,
            principal_balance: row.remove_field_by_name("principal_balance")?.try_into()?,
            interest_accrued: row.remove_field_by_name("interest_accrued")?.try_into()?,
            interest_overdue: row.remove_field_by_name("interest_overdue")?.try_into()?,
            principal_overdue: row.remove_field_by_name("principal_overdue")?.try_into()?,
            penalty_accrued: row.remove_field_by_name("penalty_accrued")?.try_into()?,
            provision_amount: row.remove_field_by_name("provision_amount")?.try_into()?,
            product_type: row.remove_field_by_name("product_type")?.try_into()?,
            status: row.remove_field_by_name("status")?.try_into()?,
            opened_at: row.remove_field_by_name("opened_at")?.try_into()?,
            updated_at: row.remove_field_by_name("updated_at")?.try_into()?,
        }))
    }
}