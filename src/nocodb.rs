use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use log::{info, error};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct NocoDBClient {
    client: Client,
    base_url: String,
    token: String,
    table_id: String,
    clubs_table_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NocoDBRecord {
    pub data: Value,
}

impl NocoDBClient {
    pub fn new(base_url: String, token: String, table_id: String, clubs_table_id: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
            token,
            table_id,
            clubs_table_id,
        }
    }

    /// Fetch all records from the NocoDB table with pagination
    pub async fn fetch_records(&self) -> Result<Vec<Value>> {
        info!("Fetching all records from NocoDB table: {}", self.table_id);
        
        let mut all_records = Vec::new();
        let mut offset = 0;
        let limit = 100; // Fetch 100 records per request
        
        loop {
            let url = format!("{}/api/v2/tables/{}/records?limit={}&offset={}", 
                self.base_url, self.table_id, limit, offset);

            info!("Requesting URL (offset={}): {}", offset, url);

            let response = self.client
                .get(&url)
                .header("xc-token", &self.token)
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let error_text = response.text().await.unwrap_or_else(|_| "Unable to read error".to_string());
                error!("Failed to fetch records: {} - {}", status, error_text);
                anyhow::bail!("Failed to fetch records from NocoDB: {} - {}", status, error_text);
            }

            let data: Value = response.json().await?;
            
            // NocoDB returns data in 'list' or 'data' field depending on version
            let records = if let Some(list) = data.get("list") {
                list.as_array()
                    .unwrap_or(&Vec::new())
                    .clone()
            } else if let Some(data) = data.get("data") {
                data.as_array()
                    .unwrap_or(&Vec::new())
                    .clone()
            } else {
                Vec::new()
            };

            let records_count = records.len();
            all_records.extend(records);
            
            info!("Fetched {} records at offset {}, total so far: {}", records_count, offset, all_records.len());
            
            // If we got fewer records than limit, we've reached the end
            if records_count < limit {
                break;
            }
            
            offset += limit;
        }

        info!("Fetched total {} records", all_records.len());
        Ok(all_records)
    }

    /// Fetch records with filters and pagination
    pub async fn fetch_records_filtered(&self, filters: &str) -> Result<Vec<Value>> {
        info!("Fetching filtered records from NocoDB");
        
        let mut all_records = Vec::new();
        let mut offset = 0;
        let limit = 100; // Fetch 100 records per request
        
        loop {
            // NocoDB API v2 format with query parameters
            let url = format!("{}/api/v2/tables/{}/records?where={}&limit={}&offset={}", 
                self.base_url, self.table_id, filters, limit, offset);

            info!("Requesting URL with filters (offset={}): {}", offset, url);

            let response = self.client
                .get(&url)
                .header("xc-token", &self.token)
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let error_text = response.text().await.unwrap_or_else(|_| "Unable to read error".to_string());
                error!("Failed to fetch filtered records: {} - {}", status, error_text);
                anyhow::bail!("Failed to fetch filtered records from NocoDB: {} - {}", status, error_text);
            }

            let data: Value = response.json().await?;
            let records = if let Some(list) = data.get("list") {
                list.as_array()
                    .unwrap_or(&Vec::new())
                    .clone()
            } else if let Some(data) = data.get("data") {
                data.as_array()
                    .unwrap_or(&Vec::new())
                    .clone()
            } else {
                Vec::new()
            };

            let records_count = records.len();
            all_records.extend(records);
            
            info!("Fetched {} filtered records at offset {}, total so far: {}", records_count, offset, all_records.len());
            
            // If we got fewer records than limit, we've reached the end
            if records_count < limit {
                break;
            }
            
            offset += limit;
        }

        info!("Fetched total {} filtered records", all_records.len());
        Ok(all_records)
    }

    /// Fetch club names from clubs table
    pub async fn fetch_club_names(&self) -> Result<HashMap<String, String>> {
        info!("Fetching club names from clubs table: {}", self.clubs_table_id);
        
        let url = format!("{}/api/v2/tables/{}/records", self.base_url, self.clubs_table_id);
        
        let response = self.client
            .get(&url)
            .header("xc-token", &self.token)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unable to read error".to_string());
            error!("Failed to fetch club names: {} - {}", status, error_text);
            anyhow::bail!("Failed to fetch club names: {} - {}", status, error_text);
        }

        let data: Value = response.json().await?;
        
        let records = if let Some(list) = data.get("list") {
            list.as_array().unwrap_or(&Vec::new()).clone()
        } else if let Some(data) = data.get("data") {
            data.as_array().unwrap_or(&Vec::new()).clone()
        } else {
            Vec::new()
        };

        let mut club_map = HashMap::new();
        for record in records {
            if let Some(obj) = record.as_object() {
                if let (Some(club_id), Some(name)) = (obj.get("club_id"), obj.get("name")) {
                    if let (Some(id_str), Some(name_str)) = (club_id.as_str(), name.as_str()) {
                        club_map.insert(id_str.to_string(), name_str.to_string());
                    }
                }
            }
        }

        info!("Loaded {} club names", club_map.len());
        Ok(club_map)
    }
}
