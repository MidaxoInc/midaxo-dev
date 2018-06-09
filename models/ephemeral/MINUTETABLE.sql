{{ dbt_utils.date_spine(
    datepart="minute",
    start_date="to_date('01/01/2010', 'mm/dd/yyyy')",
    end_date="dateadd(week, 52, current_date)"
   )
}}
