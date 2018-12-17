select * from {{ @forecast_create_close }}
where date_part('day',ddate) = iff(last_day(ddate,'month') = last_day(current_date,'month')
                                     AND date_part('day',dateadd('day',-1,current_date))<5,date_part('day',dateadd('day',-1,current_date)),5)