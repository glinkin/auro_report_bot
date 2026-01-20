use anyhow::Result;
use std::env;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    pub telegram_token: String,
    pub nocodb_url: String,
    pub nocodb_token: String,
    pub nocodb_table_id: String,
    pub nocodb_clubs_table_id: String,
    pub allowed_user_ids: Vec<i64>,
    pub report_schedule_time: String, // Format: "HH:MM"
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenv::dotenv().ok();

        // Parse allowed user IDs from comma-separated string
        let allowed_users_str = env::var("ALLOWED_USER_IDS")
            .unwrap_or_else(|_| String::new());
        let allowed_user_ids: Vec<i64> = allowed_users_str
            .split(',')
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .collect();

        let report_schedule_time = env::var("REPORT_SCHEDULE_TIME")
            .unwrap_or_else(|_| "09:00".to_string());

        Ok(Config {
            telegram_token: env::var("TELEGRAM_BOT_TOKEN")
                .map_err(|_| ConfigError::MissingEnvVar("TELEGRAM_BOT_TOKEN".to_string()))?,
            nocodb_url: env::var("NOCODB_URL")
                .map_err(|_| ConfigError::MissingEnvVar("NOCODB_URL".to_string()))?,
            nocodb_token: env::var("NOCODB_TOKEN")
                .map_err(|_| ConfigError::MissingEnvVar("NOCODB_TOKEN".to_string()))?,
            nocodb_table_id: env::var("NOCODB_TABLE_ID")
                .map_err(|_| ConfigError::MissingEnvVar("NOCODB_TABLE_ID".to_string()))?,
            nocodb_clubs_table_id: env::var("NOCODB_CLUBS_TABLE_ID")
                .map_err(|_| ConfigError::MissingEnvVar("NOCODB_CLUBS_TABLE_ID".to_string()))?,
            allowed_user_ids,
            report_schedule_time,
        })
    }
}
