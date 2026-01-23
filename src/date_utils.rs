use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use chrono_tz::Europe::Moscow;

#[derive(Debug, Clone)]
pub struct DateRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub label: String,
}

pub enum Period {
    Today,
    Yesterday,
    Week,
    Month,
    Quarter,
    HalfYear,
    Year,
}

impl Period {
    pub fn get_date_range(&self) -> DateRange {
        let now_msk = Moscow.from_utc_datetime(&Utc::now().naive_utc());
        
        match self {
            Period::Today => {
                let start = now_msk
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Сегодня ({})", now_msk.format("%d.%m.%Y")),
                }
            }
            Period::Yesterday => {
                let yesterday = now_msk - Duration::days(1);
                let start = yesterday
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = yesterday
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Вчера ({})", yesterday.format("%d.%m.%Y")),
                }
            }
            Period::Week => {
                // Последние 7 дней
                let start_date = now_msk - Duration::days(6);
                let start = start_date
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Последние 7 дней ({} - {})", 
                        start_date.format("%d.%m.%Y"),
                        now_msk.format("%d.%m.%Y")),
                }
            }
            Period::Month => {
                // Последние 30 дней
                let start_date = now_msk - Duration::days(29);
                let start = start_date
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Последние 30 дней ({} - {})", 
                        start_date.format("%d.%m.%Y"),
                        now_msk.format("%d.%m.%Y")),
                }
            }
            Period::Quarter => {
                // Последние 90 дней
                let start_date = now_msk - Duration::days(89);
                let start = start_date
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Последние 90 дней ({} - {})", 
                        start_date.format("%d.%m.%Y"),
                        now_msk.format("%d.%m.%Y")),
                }
            }
            Period::HalfYear => {
                // Последние 180 дней
                let start_date = now_msk - Duration::days(179);
                let start = start_date
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Последние 180 дней ({} - {})", 
                        start_date.format("%d.%m.%Y"),
                        now_msk.format("%d.%m.%Y")),
                }
            }
            Period::Year => {
                // Последние 365 дней
                let start_date = now_msk - Duration::days(364);
                let start = start_date
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Последние 365 дней ({} - {})", 
                        start_date.format("%d.%m.%Y"),
                        now_msk.format("%d.%m.%Y")),
                }
            }
        }
    }
}

/// Get Moscow time for scheduler
pub fn get_moscow_time() -> DateTime<chrono_tz::Tz> {
    Moscow.from_utc_datetime(&Utc::now().naive_utc())
}

/// Check if current Moscow time matches the schedule time
pub fn is_schedule_time(schedule_time: &str) -> bool {
    let now_msk = get_moscow_time();
    let current_time = format!("{:02}:{:02}", now_msk.hour(), now_msk.minute());
    current_time == schedule_time
}
