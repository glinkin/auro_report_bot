use anyhow::Result;
use log::info;
use serde_json::Value;
use std::collections::HashSet;

use crate::config::Config;
use crate::csv_generator::CsvGenerator;
use crate::date_utils::{DateRange, Period};
use crate::nocodb::NocoDBClient;
use crate::pdf_generator::PdfGenerator;

#[derive(Debug, Clone)]
pub struct ReportStats {
    pub total_records: usize,
    pub unique_clients: usize,
    pub low_aura: usize,      // < 60
    pub normal_aura: usize,   // 60-80
    pub high_aura: usize,     // > 80
}

pub struct ReportService {
    nocodb_client: NocoDBClient,
}

impl ReportService {
    pub fn new(config: &Config) -> Self {
        let nocodb_client = NocoDBClient::new(
            config.nocodb_url.clone(),
            config.nocodb_token.clone(),
            config.nocodb_table_id.clone(),
            config.nocodb_clubs_table_id.clone(),
        );

        Self { nocodb_client }
    }

    /// Generate full report (CSV + PDF) for a given period
    pub async fn generate_report(
        &self,
        period: Period,
        output_dir: &str,
    ) -> Result<(String, String, ReportStats)> {
        let date_range = period.get_date_range();
        info!("Generating report for period: {}", date_range.label);

        // Fetch club names mapping
        let club_names = self.nocodb_client.fetch_club_names().await?;
        info!("Loaded {} club names", club_names.len());

        // Fetch data from NocoDB
        let data = self.fetch_data_for_period(&date_range).await?;

        if data.is_empty() {
            info!("No data found for the period");
        }

        // Calculate statistics
        let stats = self.calculate_stats(&data);

        // Generate CSV with club names
        let csv_filename = format!("{}/report_{}.csv", output_dir, self.get_filename_suffix(&date_range));
        let csv_path = CsvGenerator::generate(&data, &csv_filename, &club_names)?;
        info!("CSV report generated: {}", csv_path);

        // Generate PDF
        let pdf_filename = format!("{}/report_{}.pdf", output_dir, self.get_filename_suffix(&date_range));
        let pdf_path = PdfGenerator::generate(&data, &pdf_filename)?;
        info!("PDF report generated: {}", pdf_path);

        Ok((csv_path, pdf_path, stats))
    }

    /// Generate only CSV report
    pub async fn generate_csv_report(&self, period: Period, output_dir: &str) -> Result<String> {
        let date_range = period.get_date_range();
        info!("Generating CSV report for period: {}", date_range.label);

        let club_names = self.nocodb_client.fetch_club_names().await?;
        let data = self.fetch_data_for_period(&date_range).await?;
        let csv_filename = format!("{}/report_{}.csv", output_dir, self.get_filename_suffix(&date_range));
        let csv_path = CsvGenerator::generate(&data, &csv_filename, &club_names)?;
        
        Ok(csv_path)
    }

    /// Generate only PDF report
    pub async fn generate_pdf_report(&self, period: Period, output_dir: &str) -> Result<String> {
        let date_range = period.get_date_range();
        info!("Generating PDF report for period: {}", date_range.label);

        let data = self.fetch_data_for_period(&date_range).await?;
        let pdf_filename = format!("{}/report_{}.pdf", output_dir, self.get_filename_suffix(&date_range));
        let pdf_path = PdfGenerator::generate(&data, &pdf_filename)?;
        
        Ok(pdf_path)
    }

    /// Fetch data from NocoDB filtered by date range
    async fn fetch_data_for_period(&self, date_range: &DateRange) -> Result<Vec<Value>> {
        info!("Fetching records for period: {}", date_range.label);
        
        // Use NocoDB server-side filtering with proper date format
        // Format: (CreatedAt1,ge,exactDate,YYYY-MM-DD HH:MM)~and(CreatedAt1,le,exactDate,YYYY-MM-DD HH:MM)
        // Using ge (>=) and le (<=) to include boundary dates
        let start_str = date_range.start.format("%Y-%m-%d %H:%M").to_string();
        let end_str = date_range.end.format("%Y-%m-%d %H:%M").to_string();
        
        let filter = format!(
            "(CreatedAt1,ge,exactDate,{})~and(CreatedAt1,le,exactDate,{})",
            start_str, end_str
        );
        
        info!("Using filter: {}", filter);
        
        match self.nocodb_client.fetch_records_filtered(&filter).await {
            Ok(records) => {
                info!("Fetched {} records for period: {}", records.len(), date_range.label);
                Ok(records)
            },
            Err(e) => {
                info!("Server-side filtering failed ({}), fetching all records", e);
                self.nocodb_client.fetch_records().await
            }
        }
    }

    /// Calculate statistics from report data
    pub fn calculate_stats(&self, data: &[Value]) -> ReportStats {
        let mut unique_phones = HashSet::new();
        let mut low_aura = 0;
        let mut normal_aura = 0;
        let mut high_aura = 0;

        for record in data {
            if let Some(obj) = record.as_object() {
                // Count unique clients by phone
                if let Some(phone) = obj.get("phone") {
                    let phone_str = match phone {
                        Value::Number(n) => n.to_string(),
                        Value::String(s) => s.clone(),
                        _ => String::new(),
                    };
                    if !phone_str.is_empty() {
                        unique_phones.insert(phone_str);
                    }
                }

                // Parse aura percent from text_aura field
                if let Some(percent_value) = Self::extract_percent_value(obj) {
                    if percent_value < 60.0 {
                        low_aura += 1;
                    } else if percent_value <= 80.0 {
                        normal_aura += 1;
                    } else {
                        high_aura += 1;
                    }
                }
            }
        }

        ReportStats {
            total_records: data.len(),
            unique_clients: unique_phones.len(),
            low_aura,
            normal_aura,
            high_aura,
        }
    }

    /// Extract percent value from text_aura field
    fn extract_percent_value(record: &serde_json::Map<String, Value>) -> Option<f64> {
        // Try text_aura field first
        if let Some(text_aura) = record.get("text_aura") {
            if let Some(aura_obj) = text_aura.as_object() {
                if let Some(percent) = aura_obj.get("percent") {
                    if let Some(percent_str) = percent.as_str() {
                        // Parse "90%" to 90.0
                        let cleaned = percent_str.trim().trim_end_matches('%');
                        return cleaned.parse::<f64>().ok();
                    } else if let Some(percent_num) = percent.as_f64() {
                        return Some(percent_num);
                    }
                }
            } else if let Some(aura_str) = text_aura.as_str() {
                if !aura_str.is_empty() {
                    if let Ok(parsed) = serde_json::from_str::<Value>(aura_str) {
                        if let Some(obj) = parsed.as_object() {
                            if let Some(percent) = obj.get("percent") {
                                if let Some(percent_str) = percent.as_str() {
                                    let cleaned = percent_str.trim().trim_end_matches('%');
                                    return cleaned.parse::<f64>().ok();
                                } else if let Some(percent_num) = percent.as_f64() {
                                    return Some(percent_num);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Fallback to aura field
        if let Some(aura) = record.get("aura") {
            if let Some(aura_obj) = aura.as_object() {
                if let Some(percent) = aura_obj.get("percent") {
                    if let Some(percent_str) = percent.as_str() {
                        let cleaned = percent_str.trim().trim_end_matches('%');
                        return cleaned.parse::<f64>().ok();
                    } else if let Some(percent_num) = percent.as_f64() {
                        return Some(percent_num);
                    }
                }
            }
        }

        None
    }

    fn get_filename_suffix(&self, date_range: &DateRange) -> String {
        date_range.start.format("%Y%m%d").to_string()
    }
}
