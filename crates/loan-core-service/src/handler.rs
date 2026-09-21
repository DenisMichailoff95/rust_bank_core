use crate::proto::loan_core_service_server::LoanCoreService;
use crate::proto::*;
use crate::repository::{LoanAccountRecord, LoanCoreRepository};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct LoanCoreServiceImpl {
    repo: Arc<LoanCoreRepository>,
}

impl LoanCoreServiceImpl {
    pub fn new(repo: Arc<LoanCoreRepository>) -> Self {
        Self { repo }
    }
}

fn generate_loan_account_number(currency: &str, product_type: &str) -> String {
    let prefix = match product_type {
        "consumer" => "455",
        "mortgage" => "456",
        "car" => "457",
        "micro" => "458",
        _ => "459",
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
impl LoanCoreService for LoanCoreServiceImpl {
    async fn open_loan_account(
        &self,
        request: Request<OpenLoanAccountRequest>,
    ) -> Result<Response<OpenLoanAccountResponse>, Status> {
        let req = request.into_inner();

        if req.loan_id.is_empty() || req.client_id.is_empty() {
            return Err(Status::invalid_argument("loan_id and client_id are required"));
        }

        let loan_account_id = uuid::Uuid::new_v4().to_string();
        let account_number =
            generate_loan_account_number(&req.currency_code, &req.product_type);
        let now = chrono::Utc::now().timestamp();

        let record = LoanAccountRecord {
            loan_account_id: loan_account_id.clone(),
            loan_id: req.loan_id.clone(),
            client_id: req.client_id.clone(),
            currency_code: req.currency_code.clone(),
            account_number: account_number.clone(),
            principal_balance: "0.00".to_string(),
            interest_accrued: "0.00".to_string(),
            interest_overdue: "0.00".to_string(),
            principal_overdue: "0.00".to_string(),
            penalty_accrued: "0.00".to_string(),
            provision_amount: "0.00".to_string(),
            product_type: req.product_type.clone(),
            status: "active".to_string(),
            opened_at: now,
            updated_at: now,
        };

        let event_payload = serde_json::json!({
            "loan_account_id": loan_account_id,
            "loan_id": req.loan_id,
            "client_id": req.client_id,
            "currency_code": req.currency_code,
            "account_number": account_number,
        })
            .to_string();

        self.repo
            .open_loan_account(&record, &event_payload)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(OpenLoanAccountResponse {
            loan_account_id,
            account_number,
            status: "active".to_string(),
        }))
    }

    async fn disburse_loan(
        &self,
        request: Request<DisburseLoanRequest>,
    ) -> Result<Response<DisburseLoanResponse>, Status> {
        let req = request.into_inner();

        let amount = Decimal::from_str(&req.amount)
            .map_err(|_| Status::invalid_argument("invalid amount"))?;
        if amount <= Decimal::ZERO {
            return Err(Status::invalid_argument("amount must be positive"));
        }

        let event_payload = serde_json::json!({
            "loan_id": req.loan_id,
            "loan_account_id": req.loan_account_id,
            "client_account_id": req.client_account_id,
            "amount": req.amount,
            "currency_code": req.currency_code,
        })
            .to_string();

        let (tx_id, new_balance) = self
            .repo
            .disburse_loan(
                &req.loan_account_id,
                &req.client_account_id,
                amount,
                &req.currency_code,
                &req.loan_id,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(DisburseLoanResponse {
            transaction_id: tx_id,
            loan_account_balance: new_balance,
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
            "loan_id": req.loan_id,
            "loan_account_id": req.loan_account_id,
            "accrual_date": req.accrual_date,
            "annual_rate": req.annual_rate,
        })
            .to_string();

        let (tx_id, accrued, total_interest) = self
            .repo
            .accrue_interest(
                &req.loan_account_id,
                &req.loan_id,
                &req.accrual_date,
                rate,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(AccrueInterestResponse {
            transaction_id: tx_id,
            accrued_amount: accrued,
            total_interest_due: total_interest,
            status: "completed".to_string(),
        }))
    }

    async fn repay_loan(
        &self,
        request: Request<RepayLoanRequest>,
    ) -> Result<Response<RepayLoanResponse>, Status> {
        let req = request.into_inner();

        let principal = Decimal::from_str(&req.principal_amount)
            .map_err(|_| Status::invalid_argument("invalid principal_amount"))?;
        let interest = Decimal::from_str(&req.interest_amount)
            .map_err(|_| Status::invalid_argument("invalid interest_amount"))?;
        let penalty = Decimal::from_str(&req.penalty_amount)
            .map_err(|_| Status::invalid_argument("invalid penalty_amount"))?;

        let event_payload = serde_json::json!({
            "loan_id": req.loan_id,
            "loan_account_id": req.loan_account_id,
            "principal": req.principal_amount,
            "interest": req.interest_amount,
            "penalty": req.penalty_amount,
        })
            .to_string();

        let (tx_id, remaining_principal, remaining_interest) = self
            .repo
            .repay_loan(
                &req.loan_account_id,
                &req.client_account_id,
                principal,
                interest,
                penalty,
                &req.currency_code,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(RepayLoanResponse {
            transaction_id: tx_id,
            remaining_principal,
            remaining_interest,
            status: "completed".to_string(),
        }))
    }

    async fn mark_overdue(
        &self,
        request: Request<MarkOverdueRequest>,
    ) -> Result<Response<MarkOverdueResponse>, Status> {
        let req = request.into_inner();

        let principal = Decimal::from_str(&req.principal_overdue)
            .map_err(|_| Status::invalid_argument("invalid principal_overdue"))?;
        let interest = Decimal::from_str(&req.interest_overdue)
            .map_err(|_| Status::invalid_argument("invalid interest_overdue"))?;

        let event_payload = serde_json::json!({
            "loan_id": req.loan_id,
            "loan_account_id": req.loan_account_id,
            "overdue_date": req.overdue_date,
        })
            .to_string();

        let tx_id = self
            .repo
            .mark_overdue(
                &req.loan_account_id,
                principal,
                interest,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(MarkOverdueResponse {
            transaction_id: tx_id,
            status: "overdue".to_string(),
        }))
    }

    async fn calculate_provision(
        &self,
        request: Request<CalculateProvisionRequest>,
    ) -> Result<Response<CalculateProvisionResponse>, Status> {
        let req = request.into_inner();

        let event_payload = serde_json::json!({
            "loan_id": req.loan_id,
            "loan_account_id": req.loan_account_id,
            "category": req.category,
        })
            .to_string();

        let (provision, category) = self
            .repo
            .calculate_provision(
                &req.loan_account_id,
                &req.category,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(CalculateProvisionResponse {
            provision_amount: provision,
            category,
            status: "calculated".to_string(),
        }))
    }

    async fn get_loan_account_state(
        &self,
        request: Request<GetLoanAccountStateRequest>,
    ) -> Result<Response<GetLoanAccountStateResponse>, Status> {
        let req = request.into_inner();

        let record = self
            .repo
            .get_loan_account_state(&req.loan_account_id)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?
            .ok_or_else(|| Status::not_found("Loan account not found"))?;

        Ok(Response::new(GetLoanAccountStateResponse {
            loan_account_id: record.loan_account_id,
            loan_id: record.loan_id,
            client_id: record.client_id,
            currency_code: record.currency_code,
            principal_balance: record.principal_balance,
            interest_accrued: record.interest_accrued,
            interest_overdue: record.interest_overdue,
            principal_overdue: record.principal_overdue,
            penalty_accrued: record.penalty_accrued,
            provision_amount: record.provision_amount,
            status: record.status,
            opened_at: record.opened_at.to_string(),
        }))
    }
}