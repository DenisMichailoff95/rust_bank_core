use crate::proto::client_service_server::ClientService;
use crate::proto::*;
use crate::repository::{ClientRecord, ClientRepository};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct ClientServiceImpl {
    repo: Arc<ClientRepository>,
}

impl ClientServiceImpl {
    pub fn new(repo: Arc<ClientRepository>) -> Self {
        Self { repo }
    }
}

#[tonic::async_trait]
impl ClientService for ClientServiceImpl {
    async fn create_client(
        &self,
        request: Request<CreateClientRequest>,
    ) -> Result<Response<CreateClientResponse>, Status> {
        let req = request.into_inner();

        if req.last_name.is_empty() || req.first_name.is_empty() {
            return Err(Status::invalid_argument(
                "last_name and first_name are required",
            ));
        }

        let client_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp();

        let record = ClientRecord {
            client_id: client_id.clone(),
            last_name: req.last_name.clone(),
            first_name: req.first_name.clone(),
            middle_name: if req.middle_name.is_empty() {
                None
            } else {
                Some(req.middle_name.clone())
            },
            birth_date: if req.birth_date.is_empty() {
                None
            } else {
                Some(req.birth_date.clone())
            },
            passport_series: if req.passport_series.is_empty() {
                None
            } else {
                Some(req.passport_series.clone())
            },
            passport_number: if req.passport_number.is_empty() {
                None
            } else {
                Some(req.passport_number.clone())
            },
            status: "active".to_string(),
            created_at: now,
        };

        let event_payload = serde_json::json!({
            "client_id": client_id,
            "last_name": req.last_name,
            "first_name": req.first_name,
            "status": "active",
            "created_at": now,
        })
            .to_string();

        self.repo
            .create_client(&record, &event_payload)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(CreateClientResponse {
            client_id,
            status: "active".to_string(),
        }))
    }

    async fn get_client(
        &self,
        request: Request<GetClientRequest>,
    ) -> Result<Response<GetClientResponse>, Status> {
        let req = request.into_inner();

        let record = self
            .repo
            .get_client(&req.client_id)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?
            .ok_or_else(|| Status::not_found("Client not found"))?;

        Ok(Response::new(GetClientResponse {
            client_id: record.client_id,
            last_name: record.last_name,
            first_name: record.first_name,
            middle_name: record.middle_name.unwrap_or_default(),
            birth_date: record.birth_date.unwrap_or_default(),
            passport_series: record.passport_series.unwrap_or_default(),
            passport_number: record.passport_number.unwrap_or_default(),
            status: record.status,
            phone: String::new(),
            email: String::new(),
            address: String::new(),
            created_at: record.created_at.to_string(),
        }))
    }

    async fn update_client_status(
        &self,
        request: Request<UpdateClientStatusRequest>,
    ) -> Result<Response<UpdateClientStatusResponse>, Status> {
        let req = request.into_inner();

        let allowed = ["active", "blocked", "closed"];
        if !allowed.contains(&req.new_status.as_str()) {
            return Err(Status::invalid_argument("invalid status"));
        }

        let event_payload = serde_json::json!({
            "client_id": req.client_id,
            "new_status": req.new_status,
            "reason": req.reason,
        })
            .to_string();

        let old_status = self
            .repo
            .update_status(&req.client_id, &req.new_status, &event_payload)
            .await
            .map_err(|e| Status::internal(format!("YDB error: {}", e)))?;

        Ok(Response::new(UpdateClientStatusResponse {
            client_id: req.client_id,
            old_status,
            new_status: req.new_status,
        }))
    }
}