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
            
            // Send statistics
            let stats_message = format!(
                "📊 *Ежедневный отчет*\n\n\
                📈 Всего генераций: *{}*\n\
                👥 Клиентов: *{}*\n\n\
                🔴 Низкая аура (<60%%): *{}*\n\
                🟡 Нормальная аура (60-80%%): *{}*\n\
                🟢 Высокая аура (>80%%): *{}*",
                stats.total_records,
                stats.unique_clients,
                stats.low_aura,
                stats.normal_aura,
                stats.high_aura
            );
            
            if let Err(e) = self.bot.send_message(chat_id, stats_message)
                .parse_mode(teloxide::types::ParseMode::Markdown)
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
