use anyhow::Result;
use log::{error, info};
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::InputFile;
use tokio::time::{sleep, Duration};

use crate::config::Config;
use crate::date_utils::{get_moscow_time, is_schedule_time, Period};
use crate::report_service::ReportService;

pub struct Scheduler {
    bot: Bot,
    config: Arc<Config>,
    report_service: Arc<ReportService>,
}

impl Scheduler {
    pub fn new(bot: Bot, config: Arc<Config>, report_service: Arc<ReportService>) -> Self {
        Self {
            bot,
            config,
            report_service,
        }
    }

    /// Start the scheduler loop
    pub async fn start(&self) {
        info!("Scheduler started. Will send reports at {} MSK", self.config.report_schedule_time);

        let mut last_sent_date = String::new();

        loop {
            if is_schedule_time(&self.config.report_schedule_time) {
                let today = get_moscow_time().format("%Y-%m-%d").to_string();
                
                // Check if we already sent report today
                if last_sent_date != today {
                    info!("Scheduled time reached. Sending daily reports...");
                    
                    if let Err(e) = self.send_daily_reports().await {
                        error!("Failed to send daily reports: {}", e);
                    } else {
                        last_sent_date = today;
                        info!("Daily reports sent successfully");
                    }
                }
            }

            // Check every minute
            sleep(Duration::from_secs(60)).await;
        }
    }

    /// Send daily reports to all allowed users
    async fn send_daily_reports(&self) -> Result<()> {
        if self.config.allowed_user_ids.is_empty() {
            info!("No allowed users configured. Skipping scheduled reports.");
            return Ok(());
        }

        // Generate yesterday's report
        let output_dir = "reports";
        std::fs::create_dir_all(output_dir)?;

        let (csv_path, pdf_path, stats) = self
            .report_service
            .generate_report(Period::Yesterday, output_dir)
            .await?;

        // Send to all allowed users
        for user_id in &self.config.allowed_user_ids {
            let chat_id = ChatId(*user_id);
            
            // Build club statistics section
            let mut club_stats_text = String::new();
            if !stats.club_stats.is_empty() {
                club_stats_text.push_str("\n\n📍 <b>Статистика по комплексам:</b>\n");
                for club_stat in &stats.club_stats {
                    let escaped_name = club_stat.club_name
                        .replace("&", "&amp;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;");
                    club_stats_text.push_str(&format!(
                        "\n🏢 <i>{}</i>\n   Генераций: <b>{}</b> ({:.1}%)\n   Клиентов: <b>{}</b>",
                        escaped_name,
                        club_stat.total_generations,
                        club_stat.percentage,
                        club_stat.unique_clients
                    ));
                }
            }
            
            // Build generation time section
            let generation_time_text = if stats.avg_generation_time > 0.0 {
                format!("\n\n⏱ <b>Среднее время генерации (done):</b> {:.1} сек", stats.avg_generation_time)
            } else {
                String::new()
            };
            
            // Build status statistics section
            let status_text = format!(
                "\n\n📋 <b>Статусы генераций:</b>\n   ✅ Done: <b>{}</b> ({:.1}%)\n   ⏳ Process: <b>{}</b> ({:.1}%)",
                stats.done_count,
                stats.done_percentage,
                stats.process_count,
                stats.process_percentage
            );
            
            // Send statistics
            let stats_message = format!(
                "📊 <b>Ежедневный отчет</b>\n\n\
                📈 Всего генераций: <b>{}</b>\n\
                👥 Уникальных клиентов: <b>{}</b>\n\n\
                🔴 Низкая аура (&lt;60%): <b>{}</b>\n\
                🟡 Нормальная аура (60-80%): <b>{}</b>\n\
                🟢 Высокая аура (&gt;80%): <b>{}</b>{}{}{}",
                stats.total_records,
                stats.unique_clients,
                stats.low_aura,
                stats.normal_aura,
                stats.high_aura,
                club_stats_text,
                generation_time_text,
                status_text
            );
            
            if let Err(e) = self.bot.send_message(chat_id, stats_message)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await {
                error!("Failed to send stats to user {}: {}", user_id, e);
            }
            
            match self.send_report_files(chat_id, &csv_path, &pdf_path).await {
                Ok(_) => info!("Report sent to user {}", user_id),
                Err(e) => error!("Failed to send report to user {}: {}", user_id, e),
            }
        }

        Ok(())
    }

    /// Send report files to a chat
    async fn send_report_files(
        &self,
        chat_id: ChatId,
        csv_path: &str,
        pdf_path: &str,
    ) -> Result<()> {
        // Send message
        self.bot
            .send_message(chat_id, "📊 Ежедневный отчет за сегодня")
            .await?;

        // Send CSV
        self.bot
            .send_document(chat_id, InputFile::file(csv_path))
            .await?;

        // Send PDF
        self.bot
            .send_document(chat_id, InputFile::file(pdf_path))
            .await?;

        Ok(())
    }
}
