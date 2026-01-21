use anyhow::Result;
use teloxide::prelude::*;
use teloxide::types::InputFile;
use log::{info, error};
use std::sync::Arc;

mod config;
mod nocodb;
mod csv_generator;
mod pdf_generator;
mod date_utils;
mod report_service;
mod scheduler;

use config::Config;
use date_utils::Period;
use report_service::ReportService;
use scheduler::Scheduler;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::init();
    
    // Load configuration
    let config = Arc::new(Config::from_env()?);
    info!("Configuration loaded successfully");
    info!("Allowed users: {:?}", config.allowed_user_ids);

    // Initialize bot
    let bot = Bot::new(&config.telegram_token);
    info!("Telegram bot initialized");

    // Initialize report service
    let report_service = Arc::new(ReportService::new(&config));
    
    // Create output directory
    std::fs::create_dir_all("reports")?;

    // Start scheduler in background
    let scheduler = Scheduler::new(bot.clone(), config.clone(), report_service.clone());
    tokio::spawn(async move {
        scheduler.start().await;
    });

    // Create dispatcher with command handler
    let handler = Update::filter_message()
        .branch(
            dptree::entry()
                .filter_command::<Command>()
                .endpoint(handle_command)
        );

    let config_clone = config.clone();
    let report_service_clone = report_service.clone();

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![config_clone, report_service_clone])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;

    Ok(())
}

#[derive(teloxide::macros::BotCommands, Clone)]
#[command(rename_rule = "lowercase", description = "Доступные команды:")]
enum Command {
    #[command(description = "Показать приветствие")]
    Start,
    #[command(description = "Справка по командам")]
    Help,
    #[command(description = "Отчет за сегодня")]
    Today,
    #[command(description = "Отчет за вчера")]
    Yesterday,
    #[command(description = "Отчет за текущую неделю")]
    Week,
    #[command(description = "Отчет за текущий месяц")]
    Month,
    #[command(description = "Отчет за текущий квартал")]
    Quarter,
    #[command(description = "Отчет за полугодие")]
    Halfyear,
    #[command(description = "Отчет за текущий год")]
    Year,
}

async fn handle_command(
    bot: Bot,
    msg: Message,
    cmd: Command,
    config: Arc<Config>,
    report_service: Arc<ReportService>,
) -> ResponseResult<()> {
    // Check if user is allowed
    if !config.allowed_user_ids.is_empty() && !config.allowed_user_ids.contains(&msg.chat.id.0) {
        bot.send_message(msg.chat.id, "❌ У вас нет доступа к этому боту.")
            .await?;
        return Ok(());
    }

    match cmd {
        Command::Start => {
            let welcome_text = format!(
                "👋 Привет! Я бот для генерации отчетов AuroScope.\n\n\
                🕐 Автоматические отчеты отправляются каждый день в {} МСК\n\n\
                📊 Доступные команды:\n\
                /today - Отчет за сегодня\n\
                /yesterday - Отчет за вчера\n\
                /week - Отчет за текущую неделю\n\
                /month - Отчет за текущий месяц\n\
                /quarter - Отчет за текущий квартал\n\
                /halfyear - Отчет за полугодие\n\
                /year - Отчет за текущий год\n\n\
                /help - Подробная справка",
                config.report_schedule_time
            );
            bot.send_message(msg.chat.id, welcome_text).await?;
        }
        Command::Help => {
            let help_text = format!(
                "📊 Справка по командам:\n\n\
                /today - Отчет за сегодняшний день\n\
                /yesterday - Отчет за вчерашний день\n\
                /week - Отчет с начала текущей недели\n\
                /month - Отчет с начала текущего месяца\n\
                /quarter - Отчет с начала текущего квартала\n\
                /halfyear - Отчет за текущее полугодие\n\
                /year - Отчет с начала текущего года\n\n\
                Каждая команда генерирует:\n\
                ✅ CSV файл с данными\n\
                ✅ PDF файл с графиками\n\n\
                📅 Автоматические отчеты отправляются ежедневно в {} МСК",
                config.report_schedule_time
            );
            bot.send_message(msg.chat.id, help_text).await?;
        }
        Command::Today => {
            generate_and_send_report(bot, msg.chat.id, Period::Today, report_service).await?;
        }
        Command::Yesterday => {
            generate_and_send_report(bot, msg.chat.id, Period::Yesterday, report_service).await?;
        }
        Command::Week => {
            generate_and_send_report(bot, msg.chat.id, Period::Week, report_service).await?;
        }
        Command::Month => {
            generate_and_send_report(bot, msg.chat.id, Period::Month, report_service).await?;
        }
        Command::Quarter => {
            generate_and_send_report(bot, msg.chat.id, Period::Quarter, report_service).await?;
        }
        Command::Halfyear => {
            generate_and_send_report(bot, msg.chat.id, Period::HalfYear, report_service).await?;
        }
        Command::Year => {
            generate_and_send_report(bot, msg.chat.id, Period::Year, report_service).await?;
        }
    }

    Ok(())
}

async fn generate_and_send_report(
    bot: Bot,
    chat_id: ChatId,
    period: Period,
    report_service: Arc<ReportService>,
) -> ResponseResult<()> {
    let date_range = period.get_date_range();
    
    bot.send_message(chat_id, format!("🔄 Генерирую отчет: {}", date_range.label))
        .await?;

    match report_service.generate_report(period, "reports").await {
        Ok((csv_path, pdf_path, stats)) => {
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
                format!("\n\n⏱ <b>Среднее время генерации:</b> {:.1} сек", stats.avg_generation_time)
            } else {
                String::new()
            };
            
            // Send statistics message
            let stats_message = format!(
                "📊 <b>Статистика по отчету</b>\n\n\
                📈 Всего генераций: <b>{}</b>\n\
                👥 Уникальных клиентов: <b>{}</b>\n\n\
                🔴 Низкая аура (&lt;60%): <b>{}</b>\n\
                🟡 Нормальная аура (60-80%): <b>{}</b>\n\
                🟢 Высокая аура (&gt;80%): <b>{}</b>{}{}",
                stats.total_records,
                stats.unique_clients,
                stats.low_aura,
                stats.normal_aura,
                stats.high_aura,
                club_stats_text,
                generation_time_text
            );
            
            bot.send_message(chat_id, stats_message)
                .parse_mode(teloxide::types::ParseMode::Html)
                .await?;

            bot.send_message(chat_id, "✅ Отчет готов! Отправляю файлы...")
                .await?;

            // Send CSV
            bot.send_document(chat_id, InputFile::file(&csv_path))
                .caption("📄 CSV данные")
                .await?;

            // Send PDF
            bot.send_document(chat_id, InputFile::file(&pdf_path))
                .caption("📊 PDF с графиками")
                .await?;

            bot.send_message(chat_id, "✨ Отчет успешно отправлен!")
                .await?;
        }
        Err(e) => {
            error!("Failed to generate report: {}", e);
            bot.send_message(
                chat_id,
                format!("❌ Ошибка при генерации отчета: {}", e),
            )
            .await?;
        }
    }

    Ok(())
}
