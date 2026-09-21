-- ============================================================
-- YDB: Ядро (Core Banking)
-- ============================================================

-- Справочник валют
CREATE TABLE currencies (
                            currency_code   Utf8 NOT NULL,
                            numeric_code    Uint16,
                            name            Utf8 NOT NULL,
                            decimal_places  Uint8 NOT NULL,
                            is_base         Bool NOT NULL,
                            PRIMARY KEY (currency_code)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED);

-- Справочник типов операций
CREATE TABLE operation_types (
                                 operation_code  Utf8 NOT NULL,
                                 name            Utf8 NOT NULL,
                                 direction       Utf8 NOT NULL,
                                 PRIMARY KEY (operation_code)
);

-- Клиенты
CREATE TABLE clients (
                         client_id       Utf8 NOT NULL,
                         last_name       Utf8 NOT NULL,
                         first_name      Utf8 NOT NULL,
                         middle_name     Utf8,
                         birth_date      Date,
                         passport_series Utf8,
                         passport_number Utf8,
                         status          Utf8 NOT NULL,
                         created_at      Timestamp NOT NULL,
                         PRIMARY KEY (client_id)
);

-- Доп. сведения о клиенте
CREATE TABLE clients_additional_info (
                                         client_id       Utf8 NOT NULL,
                                         phone           Utf8,
                                         email           Utf8,
                                         address         Utf8,
                                         marital_status  Utf8,
                                         citizenship     Utf8,
                                         PRIMARY KEY (client_id)
);

-- Счета
CREATE TABLE accounts (
                          account_id      Utf8 NOT NULL,
                          client_id       Utf8 NOT NULL,
                          currency_code   Utf8 NOT NULL,
                          account_number  Utf8 NOT NULL,
                          account_type    Utf8 NOT NULL,
                          balance         Decimal(20, 2) NOT NULL,
                          status          Utf8 NOT NULL,
                          opened_at       Timestamp NOT NULL,
                          PRIMARY KEY (account_id)
);

-- Транзакции (журнал проводок)
CREATE TABLE transactions (
                              account_id      Utf8 NOT NULL,
                              transaction_id  Utf8 NOT NULL,
                              operation_code  Utf8 NOT NULL,
                              amount          Decimal(20, 2) NOT NULL,
                              currency_code   Utf8 NOT NULL,
                              status          Utf8 NOT NULL,
                              created_at      Timestamp NOT NULL,
                              description     Utf8,
                              PRIMARY KEY (account_id, created_at, transaction_id)
);

-- ============================================================
-- Outbox (Transactional Outbox Pattern)
-- ============================================================
CREATE TABLE outbox (
                        event_id        Utf8 NOT NULL,
                        aggregate_type  Utf8 NOT NULL,
                        aggregate_id    Utf8 NOT NULL,
                        event_type      Utf8 NOT NULL,
                        payload         Utf8 NOT NULL,
                        status          Utf8 NOT NULL,
                        created_at      Timestamp NOT NULL,
                        sent_at         Timestamp,
                        retry_count     Uint32 NOT NULL DEFAULT 0,
                        PRIMARY KEY (event_id)
);

-- Вторичный индекс для поллера: читает только PENDING, отсортированные по created_at
ALTER TABLE outbox
    ADD INDEX idx_outbox_status_created GLOBAL
    ON (status, created_at);

-- Ссудные счета (учёт тела кредита, процентов, просрочки)
CREATE TABLE loan_accounts (
                               loan_account_id     Utf8 NOT NULL,          -- UUID ссудного счёта
                               loan_id             Utf8 NOT NULL,          -- ID договора из мидла
                               client_id           Utf8 NOT NULL,          -- UUID клиента
                               currency_code       Utf8 NOT NULL,
                               account_number      Utf8 NOT NULL,          -- 20-значный номер

    -- Балансовые компоненты
                               principal_balance   Decimal(20, 2) NOT NULL,  -- тело кредита
                               interest_accrued    Decimal(20, 2) NOT NULL,  -- начисленные проценты (к уплате)
                               interest_overdue    Decimal(20, 2) NOT NULL,  -- просроченные проценты
                               principal_overdue   Decimal(20, 2) NOT NULL,  -- просроченное тело
                               penalty_accrued     Decimal(20, 2) NOT NULL,  -- начисленные пени
                               provision_amount    Decimal(20, 2) NOT NULL,  -- резерв по МСФО

    -- Метаданные
                               product_type        Utf8 NOT NULL,
                               status              Utf8 NOT NULL,           -- active / overdue / closed
                               opened_at           Timestamp NOT NULL,
                               updated_at          Timestamp NOT NULL,

                               PRIMARY KEY (loan_account_id)
);

-- Вторичный индекс: поиск ссудного счёта по loan_id из мидла
ALTER TABLE loan_accounts
    ADD INDEX idx_loan_accounts_loan_id GLOBAL
    ON (loan_id);

-- Процентные начисления (журнал)
CREATE TABLE interest_accruals (
                                   loan_account_id     Utf8 NOT NULL,
                                   accrual_date        Date NOT NULL,
                                   accrual_id          Utf8 NOT NULL,
                                   amount              Decimal(20, 2) NOT NULL,
                                   annual_rate         Decimal(5, 2) NOT NULL,
                                   created_at          Timestamp NOT NULL,
                                   PRIMARY KEY (loan_account_id, accrual_date, accrual_id)
);

-- Резервы по МСФО (история)
CREATE TABLE provisions (
                            loan_account_id     Utf8 NOT NULL,
                            created_at          Timestamp NOT NULL,
                            provision_id        Utf8 NOT NULL,
                            category            Utf8 NOT NULL,
                            amount              Decimal(20, 2) NOT NULL,
                            PRIMARY KEY (loan_account_id, created_at, provision_id)
);

-- Счета вкладов
CREATE TABLE deposit_accounts (
                                  deposit_account_id  Utf8 NOT NULL,           -- UUID счёта вклада
                                  deposit_id          Utf8 NOT NULL,           -- ID договора из мидла
                                  client_id           Utf8 NOT NULL,
                                  currency_code       Utf8 NOT NULL,
                                  account_number      Utf8 NOT NULL,
                                  principal_balance   Decimal(20, 2) NOT NULL,  -- тело вклада
                                  interest_accrued    Decimal(20, 2) NOT NULL,  -- начисленные, но не капитализированные/не выплаченные
                                  interest_paid       Decimal(20, 2) NOT NULL,  -- выплаченные проценты
                                  annual_rate         Decimal(5, 2) NOT NULL,
                                  term_months         Uint32 NOT NULL,
                                  capitalization      Bool NOT NULL,            -- капитализировать проценты?
                                  product_type        Utf8 NOT NULL,
                                  status              Utf8 NOT NULL,            -- active / matured / closed / terminated
                                  opened_at           Timestamp NOT NULL,
                                  maturity_date       Date NOT NULL,
                                  updated_at          Timestamp NOT NULL,
                                  PRIMARY KEY (deposit_account_id)
);

ALTER TABLE deposit_accounts
    ADD INDEX idx_deposit_accounts_deposit_id GLOBAL
    ON (deposit_id);

-- Журнал начислений
CREATE TABLE deposit_accruals (
                                  deposit_account_id  Utf8 NOT NULL,
                                  accrual_date        Date NOT NULL,
                                  accrual_id          Utf8 NOT NULL,
                                  amount              Decimal(20, 2) NOT NULL,
                                  annual_rate         Decimal(5, 2) NOT NULL,
                                  created_at          Timestamp NOT NULL,
                                  PRIMARY KEY (deposit_account_id, accrual_date, accrual_id)
);

-- Журнал капитализаций
CREATE TABLE deposit_capitalizations (
                                         deposit_account_id  Utf8 NOT NULL,
                                         created_at          Timestamp NOT NULL,
                                         capitalization_id   Utf8 NOT NULL,
                                         amount              Decimal(20, 2) NOT NULL,
                                         new_body            Decimal(20, 2) NOT NULL,
                                         PRIMARY KEY (deposit_account_id, created_at, capitalization_id)
);

-- Пролонгации
CREATE TABLE deposit_prolongations (
                                       deposit_account_id  Utf8 NOT NULL,
                                       created_at          Timestamp NOT NULL,
                                       prolongation_id     Utf8 NOT NULL,
                                       old_maturity_date   Date NOT NULL,
                                       new_maturity_date   Date NOT NULL,
                                       new_annual_rate     Decimal(5, 2) NOT NULL,
                                       new_term_months     Uint32 NOT NULL,
                                       PRIMARY KEY (deposit_account_id, created_at, prolongation_id)
);

-- Продукты
CREATE TABLE products (
                          product_id      Utf8 NOT NULL,
                          product_code    Utf8 NOT NULL,          -- CONSUMER_ANN, SAVINGS_RUB
                          product_name    Utf8 NOT NULL,
                          product_type    Utf8 NOT NULL,          -- current / deposit / loan / card
                          currency_code   Utf8 NOT NULL,
                          status          Utf8 NOT NULL,          -- active / inactive
                          created_at      Timestamp NOT NULL,
                          PRIMARY KEY (product_id)
);

ALTER TABLE products
    ADD INDEX idx_products_code GLOBAL
    ON (product_code);

-- Условия продукта
CREATE TABLE product_terms (
                               product_code                    Utf8 NOT NULL,
                               min_amount                      Decimal(20, 2) NOT NULL,
                               max_amount                      Decimal(20, 2) NOT NULL,
                               min_term_months                 Uint32 NOT NULL,
                               max_term_months                 Uint32 NOT NULL,
                               base_rate                       Decimal(5, 2) NOT NULL,
                               early_termination_allowed       Bool NOT NULL,
                               early_termination_penalty_days  Uint32 NOT NULL,
                               capitalization_allowed          Bool NOT NULL,
                               top_up_allowed                  Bool NOT NULL,
                               min_balance                     Decimal(20, 2) NOT NULL,
                               PRIMARY KEY (product_code)
);

-- Тарифные планы
CREATE TABLE tariff_plans (
                              tariff_id                       Utf8 NOT NULL,
                              product_code                    Utf8 NOT NULL,
                              tariff_name                     Utf8 NOT NULL,
                              monthly_fee                     Decimal(20, 2) NOT NULL,
                              free_transactions_per_month     Uint32 NOT NULL,
                              over_limit_fee                  Decimal(20, 2) NOT NULL,
                              grace_period_days               Uint32 NOT NULL,
                              cashback_percent                Decimal(5, 2) NOT NULL,
                              PRIMARY KEY (tariff_id)
);

ALTER TABLE tariff_plans
    ADD INDEX idx_tariff_product GLOBAL
    ON (product_code);