use anyhow::Result;
use csv::Writer;
use serde_json::Value;
use std::fs::File;
use std::collections::HashMap;
use chrono::{Local, DateTime};
use chrono_tz::Europe::Moscow;
use log::info;

pub struct CsvGenerator;

impl CsvGenerator {
    /// Convert UTC datetime string to Moscow timezone
    fn convert_to_moscow_time(utc_str: &str) -> String {
        // Try to parse the UTC datetime string
        if let Ok(dt) = DateTime::parse_from_str(utc_str, "%Y-%m-%d %H:%M:%S%z") {
            // Convert to Moscow timezone
            let moscow_time = dt.with_timezone(&Moscow);
            return moscow_time.format("%Y-%m-%d %H:%M:%S").to_string();
        }
        
        // If parsing fails, return original string
        utc_str.to_string()
    }

    /// Extract percentage from text_aura field (JSON object with percent field)
    fn extract_aura_percent(record: &serde_json::Map<String, Value>) -> String {
        // Try text_aura field first - it contains JSON with percent field
        if let Some(text_aura) = record.get("text_aura") {
            // If text_aura is a JSON object, extract 'percent' field
            if let Some(aura_obj) = text_aura.as_object() {
                if let Some(percent) = aura_obj.get("percent") {
                    if let Some(percent_str) = percent.as_str() {
                        return percent_str.trim().trim_end_matches('%').to_string();
                    } else if let Some(percent_num) = percent.as_f64() {
                        return percent_num.to_string();
                    }
                }
            }
            // If text_aura is a string, try to parse it as JSON
            else if let Some(aura_str) = text_aura.as_str() {
                if !aura_str.is_empty() {
                    // Try to parse as JSON
                    if let Ok(parsed) = serde_json::from_str::<Value>(aura_str) {
                        if let Some(obj) = parsed.as_object() {
                            if let Some(percent) = obj.get("percent") {
                                if let Some(percent_str) = percent.as_str() {
                                    return percent_str.trim().trim_end_matches('%').to_string();
                                } else if let Some(percent_num) = percent.as_f64() {
                                    return percent_num.to_string();
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Fallback to aura field if text_aura doesn't have percent
        if let Some(aura) = record.get("aura") {
            if let Some(aura_obj) = aura.as_object() {
                if let Some(percent) = aura_obj.get("percent") {
                    if let Some(percent_str) = percent.as_str() {
                        return percent_str.trim().trim_end_matches('%').to_string();
                    } else if let Some(percent_num) = percent.as_f64() {
                        return percent_num.to_string();
                    }
                }
            } else if let Some(aura_str) = aura.as_str() {
                if !aura_str.is_empty() {
                    return aura_str.trim().trim_end_matches('%').to_string();
                }
            }
        }
        
        String::new()
    }

    /// Generate CSV report with specific fields for AuroScope
    pub fn generate(data: &[Value], output_path: &str, club_names: &HashMap<String, String>) -> Result<String> {
        info!("Generating CSV report to: {}", output_path);
        
        let mut file = File::create(output_path)?;
        
        // Write UTF-8 BOM for correct encoding detection on Windows/Android
        use std::io::Write;
        file.write_all(&[0xEF, 0xBB, 0xBF])?;
        
        // Use semicolon as delimiter for Windows Excel compatibility
        let mut writer = csv::WriterBuilder::new()
            .delimiter(b';')
            .from_writer(file);

        // Define headers for AuroScope report in Russian
        let headers = vec!["Телефон", "Имя", "Дата визита", "Продолжительность", "Комплекс", "Аура", "Дата рождения", "Пол"];
        writer.write_record(&headers)?;

        if data.is_empty() {
            info!("No data to write to CSV");
            writer.flush()?;
            return Ok(output_path.to_string());
        }

        // Write data rows with only specified fields
        for record in data {
            if let Some(obj) = record.as_object() {
                let row: Vec<String> = vec![
                    // phone (can be number or string)
                    obj.get("phone")
                        .map(|v| match v {
                            Value::Number(n) => n.to_string(),
                            Value::String(s) => s.clone(),
                            _ => String::new(),
                        })
                        .unwrap_or_default(),
                    // name
                    obj.get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    // date_visit (convert from UTC to Moscow time)
                    obj.get("date_visit")
                        .and_then(|v| v.as_str())
                        .map(|s| Self::convert_to_moscow_time(s))
                        .unwrap_or_default(),
                    // duration
                    obj.get("duration")
                        .map(|v| match v {
                            Value::Number(n) => n.to_string(),
                            Value::String(s) => s.clone(),
                            _ => String::new(),
                        })
                        .unwrap_or_default(),
                    // club_name (lookup club_id in club_names map)
                    obj.get("club_id")
                        .and_then(|v| v.as_str())
                        .and_then(|club_id| club_names.get(club_id))
                        .cloned()
                        .unwrap_or_else(|| {
                            // If not found, return the original club_id
                            obj.get("club_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string()
                        }),
                    // aura (extract percent from aura or text_aura)
                    Self::extract_aura_percent(obj),
                    // birth_date
                    obj.get("birth_date")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    // sex
                    obj.get("sex")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                ];
                writer.write_record(&row)?;
            }
        }

        writer.flush()?;
        info!("CSV report generated successfully with {} records", data.len());
        Ok(output_path.to_string())
    }

    /// Generate CSV with specific fields
    /// This method will be customized based on your requirements
    pub fn generate_custom(data: &[Value], fields: &[&str], output_path: &str) -> Result<String> {
        info!("Generating custom CSV report with {} fields", fields.len());
        
        let file = File::create(output_path)?;
        let mut writer = Writer::from_writer(file);

        // Write headers
        writer.write_record(fields)?;

        // Write data rows with only specified fields
        for record in data {
            if let Some(obj) = record.as_object() {
                let row: Vec<String> = fields
                    .iter()
                    .map(|&field| {
                        obj.get(field)
                            .and_then(|v| match v {
                                Value::String(s) => Some(s.clone()),
                                Value::Number(n) => Some(n.to_string()),
                                Value::Bool(b) => Some(b.to_string()),
                                Value::Null => Some(String::new()),
                                _ => Some(v.to_string()),
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                writer.write_record(&row)?;
            }
        }

        writer.flush()?;
        info!("Custom CSV report generated successfully");
        Ok(output_path.to_string())
    }

    /// Generate filename with timestamp
    pub fn generate_filename(prefix: &str) -> String {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        format!("{}_{}.csv", prefix, timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_csv_generation() {
        let data = vec![
            json!({"id": 1, "name": "Test1", "value": 100}),
            json!({"id": 2, "name": "Test2", "value": 200}),
        ];

        let result = CsvGenerator::generate(&data, "test_output.csv");
        assert!(result.is_ok());
    }
}
