use crate::proto::transaction_service_server::TransactionService;
use crate::proto::*;
use crate::repository::TransactionRepository;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct TransactionServiceImpl {
    repo: Arc<TransactionRepository>,
}

impl TransactionServiceImpl {
    pub fn new(repo: Arc<TransactionRepository>) -> Self {
        Self { repo }
    }
}

fn operation_direction(op_code: &str) -> &'static str {
    match op_code {
        "CASH_IN" | "TRANSFER_IN" | "INTEREST" | "SALARY" | "REFUND" => "credit",
        "CASH_OUT" | "TRANSFER_OUT" | "FEE" | "CARD_PAYMENT" | "LOAN_PAYMENT"
        | "FX_CONVERSION" | "HOLD" => "debit",
        _ => "debit",
    }
}

#[tonic::async_trait]
impl TransactionService for TransactionServiceImpl {
    async fn post_transaction(
        &self,
        request: Request<PostTransactionRequest>,
    ) -> Result<Response<PostTransactionResponse>, Status> {
        let req = request.into_inner();

        if req.account_id.is_empty() {
            return Err(Status::invalid_argument("account_id is required"));
        }

        let amount = Decimal::from_str(&req.amount)
            .map_err(|_| Status::invalid_argument("invalid amount"))?;

        if amount <= Decimal::ZERO {
            return Err(Status::invalid_argument("amount must be positive"));
        }

        let allowed_ops = [
            "CASH_IN", "CASH_OUT", "TRANSFER_IN", "TRANSFER_OUT", "FEE",
            "INTEREST", "CARD_PAYMENT", "LOAN_PAYMENT", "SALARY", "REFUND",
            "FX_CONVERSION", "HOLD",
        ];
        if !allowed_ops.contains(&req.operation_code.as_str()) {
            return Err(Status::invalid_argument("invalid operation_code"));
        }

        let direction = operation_direction(&req.operation_code);

        let event_payload = serde_json::json!({
            "account_id": req.account_id,
            "operation_code": req.operation_code,
            "amount": req.amount,
            "currency_code": req.currency_code,
            "direction": direction,
            "description": req.description,
        })
            .to_string();

        let (tx_id, new_balance) = self
            .repo
            .post_transaction(
                &req.account_id,
                &req.operation_code,
                amount,
                &req.currency_code,
                if req.description.is_empty() {
                    None
                } else {
                    Some(&req.description)
                },
                direction,
                &event_payload,
            )
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(PostTransactionResponse {
            transaction_id: tx_id,
            new_balance,
            status: "completed".to_string(),
        }))
    }

    async fn get_transaction(
        &self,
        request: Request<GetTransactionRequest>,
    ) -> Result<Response<GetTransactionResponse>, Status> {
        let req = request.into_inner();

        let record = self
            .repo
            .get_transaction(&req.account_id, &req.transaction_id)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?
            .ok_or_else(|| Status::not_found("Transaction not found"))?;

        let direction = operation_direction(&record.operation_code).to_string();

        Ok(Response::new(GetTransactionResponse {
            transaction_id: record.transaction_id,
            account_id: record.account_id,
            operation_code: record.operation_code,
            operation_name: String::new(),
            direction,
            amount: record.amount,
            currency_code: record.currency_code,
            status: record.status,
            created_at: record.created_at.to_string(),
            description: record.description.unwrap_or_default(),
        }))
    }

    async fn list_account_transactions(
        &self,
        request: Request<ListAccountTransactionsRequest>,
    ) -> Result<Response<ListAccountTransactionsResponse>, Status> {
        let req = request.into_inner();

        let records = self
            .repo
            .list_account_transactions(&req.account_id, req.limit)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        let transactions = records
            .into_iter()
            .map(|r| {
                let direction = operation_direction(&r.operation_code).to_string();
                GetTransactionResponse {
                    transaction_id: r.transaction_id,
                    account_id: r.account_id,
                    operation_code: r.operation_code,
                    operation_name: String::new(),
                    direction,
                    amount: r.amount,
                    currency_code: r.currency_code,
                    status: r.status,
                    created_at: r.created_at.to_string(),
                    description: r.description.unwrap_or_default(),
                }
            })
            .collect();

        Ok(Response::new(ListAccountTransactionsResponse { transactions }))
    }
}