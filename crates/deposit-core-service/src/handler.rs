use crate::proto::deposit_core_service_server::DepositCoreService;
use crate::proto::*;
use crate::repository::{DepositAccountRecord, DepositCoreRepository};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct DepositCoreServiceImpl {
    repo: Arc<DepositCoreRepository>,
}

impl DepositCoreServiceImpl {
    pub fn new(repo: Arc<DepositCoreRepository>) -> Self {
        Self { repo }
    }
}

fn generate_deposit_account_number(currency: &str, product_type: &str) -> String {
    let prefix = match product_type {
        "savings" => "423",
        "term" => "423",
        "accumulative" => "424",
        _ => "423",
    };
    let currency_part = match currency {
        "RUB" => "810",
        "USD" => "840",
        "EUR" => "978",
        _ => "810",
    };
    let random_part: u64 = rand::random::<u64>() % 1_000_000_000;
    format!("{}{}{:09}", prefix, currency_part, random_part)
}

#[tonic::async_trait]
impl DepositCoreService for DepositCoreServiceImpl {
    async fn open_deposit(
        &self,
        request: Request<OpenDepositRequest>,
    ) -> Result<Response<OpenDepositResponse>, Status> {
        let req = request.into_inner();

        if req.deposit_id.is_empty() || req.client_id.is_empty() {
            return Err(Status::invalid_argument("deposit_id and client_id are required"));
        }

        let amount = Decimal::from_str(&req.amount)
            .map_err(|_| Status::invalid_argument("invalid amount"))?;
        if amount <= Decimal::ZERO {
            return Err(Status::invalid_argument("amount must be positive"));
        }

        let rate = Decimal::from_str(&req.annual_rate)
            .map_err(|_| Status::invalid_argument("invalid annual_rate"))?;

        let term_months: u32 = req
            .term_months
            .parse()
            .map_err(|_| Status::invalid_argument("invalid term_months"))?;

        let deposit_account_id = uuid::Uuid::new_v4().to_string();
        let account_number =
            generate_deposit_account_number(&req.currency_code, &req.product_type);
        let now = chrono::Utc::now().timestamp();
        let maturity_date = (chrono::Utc::now() + chrono::Duration::days(term_months as i64 * 30))
            .format("%Y-%m-%d")
            .to_string();

        let record = DepositAccountRecord {
            deposit_account_id: deposit_account_id.clone(),
            deposit_id: req.deposit_id.clone(),
            client_id: req.client_id.clone(),
            currency_code: req.currency_code.clone(),
            account_number: account_number.clone(),
            principal_balance: "0.00".to_string(),
            interest_accrued: "0.00".to_string(),
            interest_paid: "0.00".to_string(),
            annual_rate: req.annual_rate.clone(),
            term_months,
            capitalization: req.capitalization,
            product_type: req.product_type.clone(),
            status: "active".to_string(),
            opened_at: now,
            maturity_date: maturity_date.clone(),
            updated_at: now,
        };

        let event_payload = serde_json::json!({
            "deposit_account_id": deposit_account_id,
            "deposit_id": req.deposit_id,
            "client_id": req.client_id,
            "amount": req.amount,
            "currency_code": req.currency_code,
            "annual_rate": req.annual_rate,
            "term_months": term_months,
            "maturity_date": maturity_date,
        })
            .to_string();

        self.repo
            .open_deposit(&record, &req.client_account_id, amount, &event_payload)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(OpenDepositResponse {
            deposit_account_id,
            account_number,
            maturity_date,
            status: "active".to_string(),
        }))
    }

    async fn top_up_deposit(
        &self,
        request: Request<TopUpDepositRequest>,
    ) -> Result<Response<TopUpDepositResponse>, Status> {
        let req = request.into_inner();

        let amount = Decimal::from_str(&req.amount)
            .map_err(|_| Status::invalid_argument("invalid amount"))?;
        if amount <= Decimal::ZERO {
            return Err(Status::invalid_argument("amount must be positive"));
        }

        let event_payload = serde_json::json!({
            "deposit_id": req.deposit_id,
            "deposit_account_id": req.deposit_account_id,
            "amount": req.amount,
        })
            .to_string();

        let (tx_id, new_balance) = self
            .repo
            .top_up(
                &req.deposit_account_id,
                &req.client_account_id,
                amount,
                &req.currency_code,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(TopUpDepositResponse {
            transaction_id: tx_id,
            new_balance,
            status: "completed".to_string(),
        }))
    }

    async fn accrue_interest(
        &self,
        request: Request<AccrueInterestRequest>,
    ) -> Result<Response<AccrueInterestResponse>, Status> {
        let req = request.into_inner();

        let rate = Decimal::from_str(&req.annual_rate)
            .map_err(|_| Status::invalid_argument("invalid annual_rate"))?;

        let event_payload = serde_json::json!({
            "deposit_id": req.deposit_id,
            "deposit_account_id": req.deposit_account_id,
            "accrual_date": req.accrual_date,
        })
            .to_string();

        let (tx_id, accrued, total) = self
            .repo
            .accrue_interest(
                &req.deposit_account_id,
                &req.deposit_id,
                &req.accrual_date,
                rate,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(AccrueInterestResponse {
            transaction_id: tx_id,
            accrued_amount: accrued,
            total_interest_accrued: total,
            status: "completed".to_string(),
        }))
    }

    async fn capitalize_interest(
        &self,
        request: Request<CapitalizeInterestRequest>,
    ) -> Result<Response<CapitalizeInterestResponse>, Status> {
        let req = request.into_inner();

        let event_payload = serde_json::json!({
            "deposit_id": req.deposit_id,
            "deposit_account_id": req.deposit_account_id,
        })
            .to_string();

        let (tx_id, capitalized, new_body) = self
            .repo
            .capitalize_interest(
                &req.deposit_account_id,
                &req.deposit_id,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(CapitalizeInterestResponse {
            transaction_id: tx_id,
            capitalized_amount: capitalized,
            new_body,
            status: "completed".to_string(),
        }))
    }

    async fn pay_out_interest(
        &self,
        request: Request<PayOutInterestRequest>,
    ) -> Result<Response<PayOutInterestResponse>, Status> {
        let req = request.into_inner();

        let event_payload = serde_json::json!({
            "deposit_id": req.deposit_id,
            "deposit_account_id": req.deposit_account_id,
        })
            .to_string();

        let (tx_id, paid) = self
            .repo
            .pay_out_interest(
                &req.deposit_account_id,
                &req.deposit_id,
                &req.client_account_id,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(PayOutInterestResponse {
            transaction_id: tx_id,
            paid_amount: paid,
            status: "completed".to_string(),
        }))
    }

    async fn early_terminate(
        &self,
        request: Request<EarlyTerminateRequest>,
    ) -> Result<Response<EarlyTerminateResponse>, Status> {
        let req = request.into_inner();

        let rate = Decimal::from_str(&req.early_rate)
            .map_err(|_| Status::invalid_argument("invalid early_rate"))?;

        let event_payload = serde_json::json!({
            "deposit_id": req.deposit_id,
            "deposit_account_id": req.deposit_account_id,
            "termination_date": req.termination_date,
        })
            .to_string();

        let (tx_id, body, interest) = self
            .repo
            .early_terminate(
                &req.deposit_account_id,
                &req.deposit_id,
                &req.client_account_id,
                rate,
                &req.termination_date,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(EarlyTerminateResponse {
            transaction_id: tx_id,
            returned_body: body,
            returned_interest: interest,
            status: "terminated".to_string(),
        }))
    }

    async fn prolong_deposit(
        &self,
        request: Request<ProlongDepositRequest>,
    ) -> Result<Response<ProlongDepositResponse>, Status> {
        let req = request.into_inner();

        let rate = Decimal::from_str(&req.new_annual_rate)
            .map_err(|_| Status::invalid_argument("invalid new_annual_rate"))?;

        let term: u32 = req
            .new_term_months
            .parse()
            .map_err(|_| Status::invalid_argument("invalid new_term_months"))?;

        let event_payload = serde_json::json!({
            "deposit_id": req.deposit_id,
            "deposit_account_id": req.deposit_account_id,
            "new_term_months": term,
            "new_annual_rate": req.new_annual_rate,
        })
            .to_string();

        let new_maturity = self
            .repo
            .prolong(
                &req.deposit_account_id,
                &req.deposit_id,
                term,
                rate,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(ProlongDepositResponse {
            new_maturity_date: new_maturity,
            status: "active".to_string(),
        }))
    }

    async fn close_deposit(
        &self,
        request: Request<CloseDepositRequest>,
    ) -> Result<Response<CloseDepositResponse>, Status> {
        let req = request.into_inner();

        let event_payload = serde_json::json!({
            "deposit_id": req.deposit_id,
            "deposit_account_id": req.deposit_account_id,
        })
            .to_string();

        let (tx_id, body, interest) = self
            .repo
            .close(
                &req.deposit_account_id,
                &req.deposit_id,
                &req.client_account_id,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(CloseDepositResponse {
            transaction_id: tx_id,
            returned_body: body,
            returned_interest: interest,
            status: "closed".to_string(),
        }))
    }

    async fn get_deposit_state(
        &self,
        request: Request<GetDepositStateRequest>,
    ) -> Result<Response<GetDepositStateResponse>, Status> {
        let req = request.into_inner();

        let record = self
            .repo
            .get_deposit_state(&req.deposit_account_id)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?
            .ok_or_else(|| Status::not_found("Deposit not found"))?;

        Ok(Response::new(GetDepositStateResponse {
            deposit_account_id: record.deposit_account_id,
            deposit_id: record.deposit_id,
            client_id: record.client_id,
            currency_code: record.currency_code,
            principal_balance: record.principal_balance,
            interest_accrued: record.interest_accrued,
            interest_paid: record.interest_paid,
            annual_rate: record.annual_rate,
            term_months: record.term_months.to_string(),
            capitalization: record.capitalization,
            opened_at: record.opened_at.to_string(),
            maturity_date: record.maturity_date,
            status: record.status,
        }))
    }
}