use crate::cache::{ProductData, ProductTermsData, TariffPlanData};
use common::config::YdbConfig;
use common::ydb_client::create_ydb_client;
use ydb::{Client, Query, YdbOrCustomerError};

type RepoResult<T> = Result<T, YdbOrCustomerError>;

pub struct ProductRepository {
    pub client: Client,
}

impl ProductRepository {
    pub async fn new(config: &YdbConfig) -> RepoResult<Self> {
        let client = create_ydb_client(config)
            .await
            .map_err(YdbOrCustomerError::from)?;
        Ok(Self { client })
    }

    pub async fn load_products(&self) -> RepoResult<Vec<ProductData>> {
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| async move {
                let res = t
                    .query(Query::from(
                        "SELECT product_id, product_code, product_name, product_type, currency_code, status, created_at FROM products",
                    ))
                    .await?;
                Ok(res)
            })
            .await?;

        let mut items = Vec::new();
        for mut row in result.into_only_result()?.rows() {
            items.push(ProductData {
                product_id: row.remove_field_by_name("product_id")?.try_into()?,
                product_code: row.remove_field_by_name("product_code")?.try_into()?,
                product_name: row.remove_field_by_name("product_name")?.try_into()?,
                product_type: row.remove_field_by_name("product_type")?.try_into()?,
                currency_code: row.remove_field_by_name("currency_code")?.try_into()?,
                status: row.remove_field_by_name("status")?.try_into()?,
                created_at: row.remove_field_by_name("created_at")?.try_into()?,
            });
        }
        Ok(items)
    }

    pub async fn load_terms(&self) -> RepoResult<Vec<ProductTermsData>> {
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| async move {
                let res = t
                    .query(Query::from(
                        "SELECT product_code, min_amount, max_amount, min_term_months, max_term_months, base_rate, \
                         early_termination_allowed, early_termination_penalty_days, capitalization_allowed, top_up_allowed, min_balance \
                         FROM product_terms",
                    ))
                    .await?;
                Ok(res)
            })
            .await?;

        let mut items = Vec::new();
        for mut row in result.into_only_result()?.rows() {
            items.push(ProductTermsData {
                product_code: row.remove_field_by_name("product_code")?.try_into()?,
                min_amount: row.remove_field_by_name("min_amount")?.try_into()?,
                max_amount: row.remove_field_by_name("max_amount")?.try_into()?,
                min_term_months: {
                    let v: i64 = row
                        .remove_field_by_name("min_term_months")?
                        .try_into()?;
                    v as u32
                },
                max_term_months: {
                    let v: i64 = row
                        .remove_field_by_name("max_term_months")?
                        .try_into()?;
                    v as u32
                },
                base_rate: row.remove_field_by_name("base_rate")?.try_into()?,
                early_termination_allowed: row
                    .remove_field_by_name("early_termination_allowed")?
                    .try_into()?,
                early_termination_penalty_days: {
                    let v: i64 = row
                        .remove_field_by_name("early_termination_penalty_days")?
                        .try_into()?;
                    v as u32
                },
                capitalization_allowed: row
                    .remove_field_by_name("capitalization_allowed")?
                    .try_into()?,
                top_up_allowed: row
                    .remove_field_by_name("top_up_allowed")?
                    .try_into()?,
                min_balance: row.remove_field_by_name("min_balance")?.try_into()?,
            });
        }
        Ok(items)
    }

    pub async fn load_tariffs(&self) -> RepoResult<Vec<TariffPlanData>> {
        let result = self
            .client
            .table_client()
            .retry_transaction(|mut t| async move {
                let res = t
                    .query(Query::from(
                        "SELECT tariff_id, product_code, tariff_name, monthly_fee, free_transactions_per_month, \
                         over_limit_fee, grace_period_days, cashback_percent FROM tariff_plans",
                    ))
                    .await?;
                Ok(res)
            })
            .await?;

        let mut items = Vec::new();
        for mut row in result.into_only_result()?.rows() {
            items.push(TariffPlanData {
                tariff_id: row.remove_field_by_name("tariff_id")?.try_into()?,
                product_code: row.remove_field_by_name("product_code")?.try_into()?,
                tariff_name: row.remove_field_by_name("tariff_name")?.try_into()?,
                monthly_fee: row.remove_field_by_name("monthly_fee")?.try_into()?,
                free_transactions_per_month: {
                    let v: i64 = row
                        .remove_field_by_name("free_transactions_per_month")?
                        .try_into()?;
                    v as u32
                },
                over_limit_fee: row
                    .remove_field_by_name("over_limit_fee")?
                    .try_into()?,
                grace_period_days: {
                    let v: i64 = row
                        .remove_field_by_name("grace_period_days")?
                        .try_into()?;
                    v as u32
                },
                cashback_percent: row
                    .remove_field_by_name("cashback_percent")?
                    .try_into()?,
            });
        }
        Ok(items)
    }
}