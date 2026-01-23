use anyhow::Result;
use log::info;
use serde_json::Value;
use std::collections::{HashSet, HashMap};

use crate::config::Config;
use crate::csv_generator::CsvGenerator;
use crate::date_utils::{DateRange, Period};
use crate::nocodb::NocoDBClient;
use crate::pdf_generator::PdfGenerator;

#[derive(Debug, Clone)]
pub struct ClubStats {
    pub club_id: String,
    pub club_name: String,
    pub total_generations: usize,
    pub unique_clients: usize,
    pub percentage: f64,
}

#[derive(Debug, Clone)]
pub struct ReportStats {
    pub total_records: usize,
    pub unique_clients: usize,
    pub low_aura: usize,      // < 60
    pub normal_aura: usize,   // 60-80
    pub high_aura: usize,     // > 80
    pub club_stats: Vec<ClubStats>,
    pub avg_generation_time: f64,  // Average time in seconds
}

pub struct ReportService {
    nocodb_client: NocoDBClient,
    date_field_name: String,
}

impl ReportService {
    pub fn new(config: &Config) -> Self {
        let nocodb_client = NocoDBClient::new(
            config.nocodb_url.clone(),
            config.nocodb_token.clone(),
            config.nocodb_table_id.clone(),
            config.nocodb_clubs_table_id.clone(),
        );

        Self { 
            nocodb_client,
            date_field_name: config.date_field_name.clone(),
        }
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
        let stats = self.calculate_stats(&data, &club_names);

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
            "({},ge,exactDate,{})~and({},le,exactDate,{})",
            self.date_field_name, start_str, self.date_field_name, end_str
        );
        
        info!("Using filter: {}", filter);
        
        match self.nocodb_client.fetch_records_filtered(&filter).await {
            Ok(records) => {
                info!("Fetched {} records for period: {}", records.len(), date_range.label);
                Ok(records)
            },
            Err(e) => {
                info!("Server-side filtering failed ({}), fetching all records and filtering client-side", e);
                let all_records = self.nocodb_client.fetch_records().await?;
                let total_count = all_records.len();
                
                // Client-side filtering by date
                let filtered_records: Vec<Value> = all_records
                    .into_iter()
                    .filter(|record| {
                        if let Some(obj) = record.as_object() {
                            // Use configured date field name
                            let created_at = obj.get(&self.date_field_name)
                                .and_then(|v| v.as_str());
                            
                            if let Some(date_str) = created_at {
                                // Try to parse the date
                                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S") {
                                    let date = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
                                    return date >= date_range.start && date <= date_range.end;
                                } else if let Ok(dt) = chrono::DateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S%z") {
                                    return dt.with_timezone(&chrono::Utc) >= date_range.start && 
                                           dt.with_timezone(&chrono::Utc) <= date_range.end;
                                }
                            }
                        }
                        false
                    })
                    .collect();
                
                info!("Filtered {} records from {} total for period: {}", 
                    filtered_records.len(), total_count, date_range.label);
                Ok(filtered_records)
            }
        }
    }

    /// Calculate statistics from report data
    pub fn calculate_stats(&self, data: &[Value], club_names: &HashMap<String, String>) -> ReportStats {
        let mut unique_phones = HashSet::new();
        let mut low_aura = 0;
        let mut normal_aura = 0;
        let mut high_aura = 0;
        
        // Statistics by club
        let mut club_generations: HashMap<String, usize> = HashMap::new();
        let mut club_unique_phones: HashMap<String, HashSet<String>> = HashMap::new();
        
        // Generation time tracking
        let mut total_generation_time = 0.0;
        let mut generation_time_count = 0;

        for record in data {
            if let Some(obj) = record.as_object() {
                // Check if club_id exists and is in club_names table
                let club_id_opt = obj.get("club_id").and_then(|v| v.as_str());
                let has_valid_club = club_id_opt.map(|id| club_names.contains_key(id)).unwrap_or(false);
                
                // Skip records without valid club_id
                if !has_valid_club {
                    continue;
                }
                
                // Count unique clients by phone
                let phone_str = if let Some(phone) = obj.get("phone") {
                    match phone {
                        Value::Number(n) => n.to_string(),
                        Value::String(s) => s.clone(),
                        _ => String::new(),
                    }
                } else {
                    String::new()
                };
                
                if !phone_str.is_empty() {
                    unique_phones.insert(phone_str.clone());
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
                
                // Count by club_id
                if let Some(club_id) = club_id_opt {
                    *club_generations.entry(club_id.to_string()).or_insert(0) += 1;
                    
                    if !phone_str.is_empty() {
                        club_unique_phones
                            .entry(club_id.to_string())
                            .or_insert_with(HashSet::new)
                            .insert(phone_str.clone());
                    }
                }
                
                // Calculate generation time (difference between UpdatedAt and CreatedAt)
                if let (Some(created), Some(updated)) = (
                    obj.get("CreatedAt").or_else(|| obj.get("CreatedAt1")).and_then(|v| v.as_str()),
                    obj.get("UpdatedAt").or_else(|| obj.get("UpdatedAt1")).and_then(|v| v.as_str())
                ) {
                    if let (Ok(created_time), Ok(updated_time)) = (
                        chrono::DateTime::parse_from_str(created, "%Y-%m-%d %H:%M:%S%z")
                            .or_else(|_| chrono::NaiveDateTime::parse_from_str(created, "%Y-%m-%d %H:%M:%S")
                                .map(|dt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc).fixed_offset())),
                        chrono::DateTime::parse_from_str(updated, "%Y-%m-%d %H:%M:%S%z")
                            .or_else(|_| chrono::NaiveDateTime::parse_from_str(updated, "%Y-%m-%d %H:%M:%S")
                                .map(|dt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc).fixed_offset()))
                    ) {
                        let duration = updated_time.signed_duration_since(created_time);
                        total_generation_time += duration.num_milliseconds() as f64 / 1000.0;
                        generation_time_count += 1;
                    }
                }
            }
        }

        // Calculate club statistics - only for clubs that exist in club_names
        let total_records: usize = club_generations.values().sum();
        let mut club_stats: Vec<ClubStats> = club_generations
            .iter()
            .filter(|(club_id, _)| club_names.contains_key(*club_id))
            .map(|(club_id, &generations)| {
                let unique_clients = club_unique_phones
                    .get(club_id)
                    .map(|phones| phones.len())
                    .unwrap_or(0);
                
                let percentage = if total_records > 0 {
                    (generations as f64 / total_records as f64) * 100.0
                } else {
                    0.0
                };
                
                ClubStats {
                    club_id: club_id.clone(),
                    club_name: club_names.get(club_id).cloned().unwrap_or_else(|| club_id.clone()),
                    total_generations: generations,
                    unique_clients,
                    percentage,
                }
            })
            .collect();
        
        // Sort by total_generations descending
        club_stats.sort_by(|a, b| b.total_generations.cmp(&a.total_generations));
        
        let avg_generation_time = if generation_time_count > 0 {
            total_generation_time / generation_time_count as f64
        } else {
            0.0
        };

        ReportStats {
            total_records,
            unique_clients: unique_phones.len(),
            low_aura,
            normal_aura,
            high_aura,
            club_stats,
            avg_generation_time,
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
