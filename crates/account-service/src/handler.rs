use crate::proto::account_service_server::AccountService;
use crate::proto::*;
use crate::repository::{AccountRecord, AccountRepository};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct AccountServiceImpl {
    repo: Arc<AccountRepository>,
}

impl AccountServiceImpl {
    pub fn new(repo: Arc<AccountRepository>) -> Self {
        Self { repo }
    }
}

fn generate_account_number(currency: &str, account_type: &str) -> String {
    let prefix = match account_type {
        "current" => "40817",
        "deposit" => "42307",
        "credit" => "40817",
        _ => "40817",
    };
    let currency_part = match currency {
        "RUB" => "810",
        "USD" => "840",
        "EUR" => "978",
        "CNY" => "156",
        _ => "810",
    };
    let random_part: u64 = rand::random::<u64>() % 1_000_000_000;
    format!("{}{}{:09}", prefix, currency_part, random_part)
}

#[tonic::async_trait]
impl AccountService for AccountServiceImpl {
    async fn create_account(
        &self,
        request: Request<CreateAccountRequest>,
    ) -> Result<Response<CreateAccountResponse>, Status> {
        let req = request.into_inner();

        if req.client_id.is_empty() {
            return Err(Status::invalid_argument("client_id is required"));
        }
        let allowed_types = ["current", "deposit", "credit"];
        if !allowed_types.contains(&req.account_type.as_str()) {
            return Err(Status::invalid_argument("invalid account_type"));
        }
        let allowed_currencies = ["RUB", "USD", "EUR", "CNY", "KZT", "GBP", "JPY", "CHF"];
        if !allowed_currencies.contains(&req.currency_code.as_str()) {
            return Err(Status::invalid_argument("invalid currency_code"));
        }

        let account_id = uuid::Uuid::new_v4().to_string();
        let account_number = generate_account_number(&req.currency_code, &req.account_type);
        let now = chrono::Utc::now().timestamp();

        let record = AccountRecord {
            account_id: account_id.clone(),
            client_id: req.client_id.clone(),
            currency_code: req.currency_code.clone(),
            account_number: account_number.clone(),
            account_type: req.account_type.clone(),
            balance: "0.00".to_string(),
            status: "active".to_string(),
            opened_at: now,
        };

        let event_payload = serde_json::json!({
            "account_id": account_id,
            "client_id": req.client_id,
            "currency_code": req.currency_code,
            "account_number": account_number,
            "account_type": req.account_type,
            "opened_at": now,
        })
            .to_string();

        self.repo
            .create_account(&record, &event_payload)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(CreateAccountResponse {
            account_id,
            account_number,
            status: "active".to_string(),
        }))
    }

    async fn get_account(
        &self,
        request: Request<GetAccountRequest>,
    ) -> Result<Response<GetAccountResponse>, Status> {
        let req = request.into_inner();

        let record = self
            .repo
            .get_account(&req.account_id)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?
            .ok_or_else(|| Status::not_found("Account not found"))?;

        Ok(Response::new(GetAccountResponse {
            account_id: record.account_id,
            client_id: record.client_id,
            account_number: record.account_number,
            account_type: record.account_type,
            currency_code: record.currency_code,
            balance: record.balance,
            status: record.status,
            opened_at: record.opened_at.to_string(),
        }))
    }

    async fn get_balance(
        &self,
        request: Request<GetBalanceRequest>,
    ) -> Result<Response<GetBalanceResponse>, Status> {
        let req = request.into_inner();

        let (balance, currency_code) = self
            .repo
            .get_balance(&req.account_id)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?
            .ok_or_else(|| Status::not_found("Account not found"))?;

        Ok(Response::new(GetBalanceResponse {
            account_id: req.account_id,
            balance,
            currency_code,
        }))
    }

    async fn list_client_accounts(
        &self,
        request: Request<ListClientAccountsRequest>,
    ) -> Result<Response<ListClientAccountsResponse>, Status> {
        let req = request.into_inner();

        let records = self
            .repo
            .list_client_accounts(&req.client_id)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        let accounts = records
            .into_iter()
            .map(|r| GetAccountResponse {
                account_id: r.account_id,
                client_id: r.client_id,
                account_number: r.account_number,
                account_type: r.account_type,
                currency_code: r.currency_code,
                balance: r.balance,
                status: r.status,
                opened_at: r.opened_at.to_string(),
            })
            .collect();

        Ok(Response::new(ListClientAccountsResponse { accounts }))
    }

    async fn update_account_status(
        &self,
        request: Request<UpdateAccountStatusRequest>,
    ) -> Result<Response<UpdateAccountStatusResponse>, Status> {
        let req = request.into_inner();

        let allowed = ["active", "frozen", "closed"];
        if !allowed.contains(&req.new_status.as_str()) {
            return Err(Status::invalid_argument("invalid status"));
        }

        let event_payload = serde_json::json!({
            "account_id": req.account_id,
            "new_status": req.new_status,
            "reason": req.reason,
        })
            .to_string();

        let old_status = self
            .repo
            .update_status(&req.account_id, &req.new_status, &event_payload)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(UpdateAccountStatusResponse {
            account_id: req.account_id,
            old_status,
            new_status: req.new_status,
        }))
    }
}