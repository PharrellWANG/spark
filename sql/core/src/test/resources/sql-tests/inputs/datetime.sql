-- date time functions

-- [SPARK-16836] current_date and current_timestamp literals
select current_date = current_date(), current_timestamp = current_timestamp();

select to_date(null), to_date('2016-12-31'), to_date('2016-12-31', 'yyyy-MM-dd');

select to_timestamp(null), to_timestamp('2016-12-31 00:12:00'), to_timestamp('2016-12-31', 'yyyy-MM-dd');

-- trunc date
select trunc(to_date('2015-07-22'), 'yyyy'), trunc(to_date('2015-07-22'), 'YYYY'),
  trunc(to_date('2015-07-22'), 'year'), trunc(to_date('2015-07-22'), 'YEAR'),
  trunc(to_date('2015-07-22'), 'yy'), trunc(to_date('2015-07-22'), 'YY');
select trunc(to_date('2015-07-22'), 'month'), trunc(to_date('2015-07-22'), 'MONTH'),
  trunc(to_date('2015-07-22'), 'mon'), trunc(to_date('2015-07-22'), 'MON'),
  trunc(to_date('2015-07-22'), 'mm'), trunc(to_date('2015-07-22'), 'MM');
select trunc('2015-07-22', 'DD'), trunc('2015-07-22', null), trunc(null, 'MON'), trunc(null, null);
