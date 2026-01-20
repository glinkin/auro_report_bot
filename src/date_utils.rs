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
                let start_of_week = now_msk - Duration::days(now_msk.weekday().num_days_from_monday() as i64);
                let start = start_of_week
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
                    label: format!("Текущая неделя ({} - {})", 
                        start_of_week.format("%d.%m.%Y"),
                        now_msk.format("%d.%m.%Y")),
                }
            }
            Period::Month => {
                let start_of_month = now_msk
                    .date_naive()
                    .with_day(1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start_of_month).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Текущий месяц ({})", now_msk.format("%B %Y")),
                }
            }
            Period::Quarter => {
                let current_month = now_msk.month();
                let quarter_start_month = ((current_month - 1) / 3) * 3 + 1;
                let start_of_quarter = now_msk
                    .date_naive()
                    .with_day(1)
                    .unwrap()
                    .with_month(quarter_start_month)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                let quarter_num = (quarter_start_month - 1) / 3 + 1;
                DateRange {
                    start: Moscow.from_local_datetime(&start_of_quarter).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Q{} {}", quarter_num, now_msk.format("%Y")),
                }
            }
            Period::HalfYear => {
                let current_month = now_msk.month();
                let half_start_month = if current_month <= 6 { 1 } else { 7 };
                let start_of_half = now_msk
                    .date_naive()
                    .with_day(1)
                    .unwrap()
                    .with_month(half_start_month)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                let half_num = if half_start_month == 1 { 1 } else { 2 };
                DateRange {
                    start: Moscow.from_local_datetime(&start_of_half).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Полугодие {} ({})", half_num, now_msk.format("%Y")),
                }
            }
            Period::Year => {
                let start_of_year = now_msk
                    .date_naive()
                    .with_day(1)
                    .unwrap()
                    .with_month(1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                let end = now_msk
                    .date_naive()
                    .and_hms_opt(23, 59, 59)
                    .unwrap();
                
                DateRange {
                    start: Moscow.from_local_datetime(&start_of_year).unwrap().with_timezone(&Utc),
                    end: Moscow.from_local_datetime(&end).unwrap().with_timezone(&Utc),
                    label: format!("Текущий год ({})", now_msk.format("%Y")),
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
