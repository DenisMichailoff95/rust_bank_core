use crate::cache::SharedProductCache;
use crate::proto::product_service_server::ProductService;
use crate::proto::*;
use crate::rules;
use rust_decimal::Decimal;
use std::str::FromStr;
use tonic::{Request, Response, Status};

pub struct ProductServiceImpl {
    cache: SharedProductCache,
}

impl ProductServiceImpl {
    pub fn new(cache: SharedProductCache) -> Self {
        Self { cache }
    }
}

#[tonic::async_trait]
impl ProductService for ProductServiceImpl {
    async fn get_product(
        &self,
        request: Request<GetProductRequest>,
    ) -> Result<Response<GetProductResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let p = cache
            .get_product_by_code(&req.product_code)
            .ok_or_else(|| Status::not_found("Product not found"))?
            .clone();

        Ok(Response::new(GetProductResponse {
            product: Some(Product {
                product_id: p.product_id,
                product_code: p.product_code,
                product_name: p.product_name,
                product_type: p.product_type,
                currency_code: p.currency_code,
                status: p.status,
                created_at: p.created_at,
            }),
        }))
    }

    async fn list_products(
        &self,
        request: Request<ListProductsRequest>,
    ) -> Result<Response<ListProductsResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let products = cache
            .list_products(&req.product_type, &req.currency_code)
            .into_iter()
            .map(|p| Product {
                product_id: p.product_id,
                product_code: p.product_code,
                product_name: p.product_name,
                product_type: p.product_type,
                currency_code: p.currency_code,
                status: p.status,
                created_at: p.created_at,
            })
            .collect();

        Ok(Response::new(ListProductsResponse { products }))
    }

    async fn get_product_terms(
        &self,
        request: Request<GetProductTermsRequest>,
    ) -> Result<Response<GetProductTermsResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let t = cache
            .get_terms(&req.product_code)
            .ok_or_else(|| Status::not_found("Terms not found"))?
            .clone();

        Ok(Response::new(GetProductTermsResponse {
            terms: Some(ProductTerms {
                product_code: t.product_code,
                min_amount: t.min_amount,
                max_amount: t.max_amount,
                min_term_months: t.min_term_months,
                max_term_months: t.max_term_months,
                base_rate: t.base_rate,
                early_termination_allowed: t.early_termination_allowed,
                early_termination_penalty_days: t.early_termination_penalty_days,
                capitalization_allowed: t.capitalization_allowed,
                top_up_allowed: t.top_up_allowed,
                min_balance: t.min_balance,
            }),
        }))
    }

    async fn get_tariff_plan(
        &self,
        request: Request<GetTariffPlanRequest>,
    ) -> Result<Response<GetTariffPlanResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let t = cache
            .get_tariff(&req.tariff_id)
            .ok_or_else(|| Status::not_found("Tariff not found"))?
            .clone();

        Ok(Response::new(GetTariffPlanResponse {
            tariff: Some(TariffPlan {
                tariff_id: t.tariff_id,
                product_code: t.product_code,
                tariff_name: t.tariff_name,
                monthly_fee: t.monthly_fee,
                free_transactions_per_month: t.free_transactions_per_month,
                over_limit_fee: t.over_limit_fee,
                grace_period_days: t.grace_period_days,
                cashback_percent: t.cashback_percent,
            }),
        }))
    }

    async fn generate_account_number(
        &self,
        request: Request<GenerateAccountNumberRequest>,
    ) -> Result<Response<GenerateAccountNumberResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let product = cache
            .get_product_by_code(&req.product_code)
            .ok_or_else(|| Status::not_found("Product not found"))?;

        let balance_account = rules::balance_account_for(&product.product_type).to_string();

        // В реальности seq должен быть атомарным счётчиком (например, из YDB sequence).
        // Здесь — простой генератор на основе timestamp.
        let seq = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let account_number = rules::generate_account_number(
            &product.product_type,
            &req.currency_code,
            seq,
        );

        Ok(Response::new(GenerateAccountNumberResponse {
            account_number,
            balance_account,
        }))
    }

    async fn check_eligibility(
        &self,
        request: Request<CheckEligibilityRequest>,
    ) -> Result<Response<CheckEligibilityResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let product = cache
            .get_product_by_code(&req.product_code)
            .ok_or_else(|| Status::not_found("Product not found"))?;

        let terms = cache
            .get_terms(&req.product_code)
            .ok_or_else(|| Status::not_found("Terms not found"))?;

        let requested_amount = if req.requested_amount.is_empty() {
            None
        } else {
            Some(
                Decimal::from_str(&req.requested_amount)
                    .map_err(|_| Status::invalid_argument("invalid requested_amount"))?,
            )
        };

        let (eligible, reason) = rules::check_eligibility(
            &product.product_type,
            &req.client_status,
            requested_amount,
            terms,
        );

        let conditions = if eligible {
            vec![
                format!("Минимальная сумма: {}", terms.min_amount),
                format!("Максимальная сумма: {}", terms.max_amount),
                format!("Срок: от {} до {} мес.", terms.min_term_months, terms.max_term_months),
                format!("Базовая ставка: {}%", terms.base_rate),
            ]
        } else {
            vec![]
        };

        Ok(Response::new(CheckEligibilityResponse {
            eligible,
            reason,
            conditions,
        }))
    }

    async fn calculate_rate(
        &self,
        request: Request<CalculateRateRequest>,
    ) -> Result<Response<CalculateRateResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let terms = cache
            .get_terms(&req.product_code)
            .ok_or_else(|| Status::not_found("Terms not found"))?;

        let base_rate = Decimal::from_str(&terms.base_rate)
            .map_err(|_| Status::internal("invalid base_rate in cache"))?;

        let requested_amount = if req.requested_amount.is_empty() {
            None
        } else {
            Some(
                Decimal::from_str(&req.requested_amount)
                    .map_err(|_| Status::invalid_argument("invalid requested_amount"))?,
            )
        };

        let category = if req.client_category.is_empty() {
            "standard"
        } else {
            &req.client_category
        };

        let (personal_rate, reason) =
            rules::calculate_personal_rate(base_rate, category, requested_amount);

        Ok(Response::new(CalculateRateResponse {
            base_rate: base_rate.to_string(),
            personal_rate: personal_rate.to_string(),
            discount_reason: reason,
        }))
    }
}