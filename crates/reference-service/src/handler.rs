use crate::cache::SharedCache;
use crate::proto::reference_service_server::ReferenceService;
use crate::proto::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct ReferenceServiceImpl {
    cache: SharedCache,
}

impl ReferenceServiceImpl {
    pub fn new(cache: SharedCache) -> Self {
        Self { cache }
    }
}

#[tonic::async_trait]
impl ReferenceService for ReferenceServiceImpl {
    async fn get_currency(
        &self,
        request: Request<GetCurrencyRequest>,
    ) -> Result<Response<GetCurrencyResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let currency = cache
            .get_currency(&req.currency_code)
            .ok_or_else(|| Status::not_found("Currency not found"))?
            .clone();

        Ok(Response::new(GetCurrencyResponse {
            currency: Some(Currency {
                currency_code: currency.currency_code,
                numeric_code: currency.numeric_code,
                name: currency.name,
                decimal_places: currency.decimal_places,
                is_base: currency.is_base,
            }),
        }))
    }

    async fn list_currencies(
        &self,
        _request: Request<ListCurrenciesRequest>,
    ) -> Result<Response<ListCurrenciesResponse>, Status> {
        let cache = self.cache.read().await;

        let currencies = cache
            .list_currencies()
            .into_iter()
            .map(|c| Currency {
                currency_code: c.currency_code,
                numeric_code: c.numeric_code,
                name: c.name,
                decimal_places: c.decimal_places,
                is_base: c.is_base,
            })
            .collect();

        Ok(Response::new(ListCurrenciesResponse { currencies }))
    }

    async fn get_operation_type(
        &self,
        request: Request<GetOperationTypeRequest>,
    ) -> Result<Response<GetOperationTypeResponse>, Status> {
        let req = request.into_inner();
        let cache = self.cache.read().await;

        let op = cache
            .get_operation_type(&req.operation_code)
            .ok_or_else(|| Status::not_found("Operation type not found"))?
            .clone();

        Ok(Response::new(GetOperationTypeResponse {
            operation_type: Some(OperationType {
                operation_code: op.operation_code,
                name: op.name,
                direction: op.direction,
            }),
        }))
    }

    async fn list_operation_types(
        &self,
        _request: Request<ListOperationTypesRequest>,
    ) -> Result<Response<ListOperationTypesResponse>, Status> {
        let cache = self.cache.read().await;

        let operation_types = cache
            .list_operation_types()
            .into_iter()
            .map(|o| OperationType {
                operation_code: o.operation_code,
                name: o.name,
                direction: o.direction,
            })
            .collect();

        Ok(Response::new(ListOperationTypesResponse { operation_types }))
    }

    async fn refresh_cache(
        &self,
        _request: Request<RefreshCacheRequest>,
    ) -> Result<Response<RefreshCacheResponse>, Status> {
        // Логика refresh в main.rs через отдельный task — здесь возвращаем текущее состояние.
        let cache = self.cache.read().await;
        Ok(Response::new(RefreshCacheResponse {
            currencies_count: cache.currencies_count() as i32,
            operation_types_count: cache.operation_types_count() as i32,
        }))
    }
}