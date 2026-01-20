use anyhow::Result;
use printpdf::*;
use serde_json::Value;
use std::fs::File;
use std::io::BufWriter;
use std::collections::HashMap;
use chrono::{DateTime, Timelike};
use log::info;

pub struct PdfGenerator;

impl PdfGenerator {
    /// Generate PDF report with vector charts (TradingView style)
    pub fn generate(data: &[Value], output_path: &str) -> Result<String> {
        info!("Generating PDF report with vector charts to: {}", output_path);

        // Create PDF document
        let (doc, page1, layer1) = PdfDocument::new(
            "AuroScope Report",
            Mm(210.0), // A4 width
            Mm(297.0), // A4 height
            "Layer 1",
        );

        let current_layer = doc.get_page(page1).get_layer(layer1);

        // Fonts
        let font_bold = doc.add_builtin_font(BuiltinFont::HelveticaBold)?;
        let font_regular = doc.add_builtin_font(BuiltinFont::Helvetica)?;

        // Title
        current_layer.use_text(
            "AuroScope Report",
            24.0,
            Mm(20.0),
            Mm(280.0),
            &font_bold,
        );

        // Draw hourly distribution chart
        Self::draw_hourly_chart(&current_layer, data, &font_bold, &font_regular)?;

        // Save PDF
        doc.save(&mut BufWriter::new(File::create(output_path)?))?;
        info!("PDF report with vector charts generated successfully");
        Ok(output_path.to_string())
    }

    /// Calculate statistics from data
    fn calculate_statistics(data: &[Value]) -> AuraStatistics {
        let mut low = 0;
        let mut normal = 0;
        let mut high = 0;

        for record in data {
            if let Some(obj) = record.as_object() {
                if let Some(percent) = Self::extract_percent(obj) {
                    if percent < 60.0 {
                        low += 1;
                    } else if percent <= 80.0 {
                        normal += 1;
                    } else {
                        high += 1;
                    }
                }
            }
        }

        AuraStatistics {
            total: data.len(),
            low_aura: low,
            normal_aura: normal,
            high_aura: high,
        }
    }

    /// Extract percent value from record
    fn extract_percent(record: &serde_json::Map<String, Value>) -> Option<f64> {
        if let Some(text_aura) = record.get("text_aura") {
            if let Some(aura_obj) = text_aura.as_object() {
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

    /// Draw hourly distribution chart using vector graphics (TradingView style)
    fn draw_hourly_chart(
        layer: &PdfLayerReference,
        data: &[Value],
        font_bold: &IndirectFontRef,
        font_regular: &IndirectFontRef,
    ) -> Result<()> {
        // Count records by hour
        let mut hourly_counts: HashMap<u32, u32> = HashMap::new();
        
        for record in data {
            if let Some(obj) = record.as_object() {
                if let Some(created_at) = obj.get("CreatedAt1").and_then(|v| v.as_str()) {
                    if let Ok(dt) = DateTime::parse_from_str(created_at, "%Y-%m-%d %H:%M:%S%z") {
                        let hour = dt.hour();
                        *hourly_counts.entry(hour).or_insert(0) += 1;
                    }
                }
            }
        }

        // Chart dimensions and position (using f64 for calculations, convert to f32 for Mm)
        // Ratio height:width = 1:5
        let chart_x = 10.0_f64;
        let chart_y = 220.0_f64;
        let chart_width = 180.0_f64;
        let chart_height = 36.0_f64; // 1:5 ratio

        // Find max value for scaling
        let max_count = hourly_counts.values().max().copied().unwrap_or(1);
        let max_count = if max_count == 0 { 1 } else { max_count };

        // Chart title
        layer.use_text(
            "Распределение генераций по часам",
            14.0,
            Mm(chart_x as f32),
            Mm((chart_y + chart_height + 10.0) as f32),
            font_bold,
        );

        // Chart description
        layer.use_text(
            "График показывает количество генераций ауры по часам суток (0-23 ч).",
            9.0,
            Mm(chart_x as f32),
            Mm((chart_y + chart_height + 5.0) as f32),
            font_regular,
        );
        layer.use_text(
            "Помогает выявить пиковые часы активности и оптимизировать работу комплекса.",
            9.0,
            Mm(chart_x as f32),
            Mm((chart_y + chart_height + 0.5) as f32),
            font_regular,
        );

        // TradingView colors - using RGB values (0-255 converted to 0-1)
        let bar_color = Color::Rgb(Rgb::new(0.149, 0.651, 0.604, None)); // #26A69A (teal)
        
        // Draw axes using lines
        layer.set_outline_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));
        layer.set_outline_thickness(1.0);
        
        // X-axis
        let x_axis_points = vec![
            (Point::new(Mm(chart_x as f32), Mm(chart_y as f32)), false),
            (Point::new(Mm((chart_x + chart_width) as f32), Mm(chart_y as f32)), false),
        ];
        let x_axis_line = Line {
            points: x_axis_points,
            is_closed: false,
        };
        layer.add_line(x_axis_line);

        // Y-axis
        let y_axis_points = vec![
            (Point::new(Mm(chart_x as f32), Mm(chart_y as f32)), false),
            (Point::new(Mm(chart_x as f32), Mm((chart_y + chart_height) as f32)), false),
        ];
        let y_axis_line = Line {
            points: y_axis_points,
            is_closed: false,
        };
        layer.add_line(y_axis_line);

        // Draw grid lines (light gray)
        layer.set_outline_color(Color::Rgb(Rgb::new(0.9, 0.9, 0.9, None)));
        layer.set_outline_thickness(0.3);
        
        for i in 1..=3 {
            let y = chart_y + (chart_height / 3.0) * i as f64;
            let grid_points = vec![
                (Point::new(Mm(chart_x as f32), Mm(y as f32)), false),
                (Point::new(Mm((chart_x + chart_width) as f32), Mm(y as f32)), false),
            ];
            let grid_line = Line {
                points: grid_points,
                is_closed: false,
            };
            layer.add_line(grid_line);
        }

        // Draw bars
        layer.set_fill_color(bar_color.clone());
        layer.set_outline_color(bar_color);
        layer.set_outline_thickness(0.5);

        let bar_width = chart_width / 24.0 * 0.85;
        
        for hour in 0..24 {
            let count = *hourly_counts.get(&hour).unwrap_or(&0);
            let bar_height = if count > 0 {
                (count as f64 / max_count as f64) * chart_height
            } else {
                0.0
            };
            let x = chart_x + (hour as f64 * chart_width / 24.0) + (chart_width / 24.0 * 0.075);
            
            // Draw bar if there's data
            if count > 0 {
                let bar_points = vec![
                    (Point::new(Mm(x as f32), Mm(chart_y as f32)), false),
                    (Point::new(Mm((x + bar_width) as f32), Mm(chart_y as f32)), false),
                    (Point::new(Mm((x + bar_width) as f32), Mm((chart_y + bar_height) as f32)), false),
                    (Point::new(Mm(x as f32), Mm((chart_y + bar_height) as f32)), false),
                ];
                
                layer.add_polygon(Polygon {
                    rings: vec![bar_points],
                    mode: printpdf::path::PaintMode::FillStroke,
                    winding_order: printpdf::path::WindingOrder::NonZero,
                });
            }
            
            // Draw hour label under each bar
            layer.use_text(
                &format!("{}", hour),
                6.0,
                Mm((x + bar_width / 2.0 - 1.5) as f32),
                Mm((chart_y - 3.0) as f32),
                font_regular,
            );
        }

        // Reset color for text
        layer.set_outline_color(Color::Rgb(Rgb::new(0.0, 0.0, 0.0, None)));

        // Y-axis labels (simplified - only min, mid, max)
        for i in 0..=3 {
            let value = (max_count as f64 / 3.0 * i as f64) as u32;
            let y = chart_y + (chart_height / 3.0) * i as f64;
            layer.use_text(
                &format!("{}", value),
                7.0,
                Mm((chart_x - 8.0) as f32),
                Mm((y - 1.0) as f32),
                font_regular,
            );
        }

        Ok(())
    }

    pub fn generate_filename(prefix: &str) -> String {
        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
        format!("{}_{}.pdf", prefix, timestamp)
    }
}

#[derive(Debug)]
struct AuraStatistics {
    total: usize,
    low_aura: usize,
    normal_aura: usize,
    high_aura: usize,
}
