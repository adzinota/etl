create or replace procedure dm.fill_f101_round_f(cur_oper_date date)
language plpgsql
as $procedure$
	begin
	-- Вставка полученных значений в целевую таблицу
	insert into dm.dm_f101_round_f
	(date_from, -- Начало периода
	date_to, -- Конец периода
	chapter,--PLAN организации глава Плана счетов бухгалтерского учета в кредитных организациях: А - балансовые счета; Б - счета доверительного управления; В - внебалансовые счета; Г - срочные операции
	num_sc,-- NUM_SC номер счета второго порядка
	a_p,-- A_P признак счета (1 активный/2 пассивный)
	bal_in_rub,-- Входящие остатки в рублях
	bal_in_curr,-- Входящие остатки валют
	bal_in_total,-- Входящие остатки итого
	turn_deb_rub,-- Обороты за отчетный период по дебету в рублях
	turn_deb_curr,-- Обороты за отчетный период по дебету валют
	turn_deb_total,-- Обороты за отчетный период по дебету итого
	turn_cre_rub,-- Обороты за отчетный период по кредиту в рублях
	turn_cre_curr,-- Обороты за отчетный период по кредиту валют
	turn_cre_total,-- Обороты за отчетный период по кредиту итого
	bal_out_rub,--Исходящие остатки (в рублях)
	bal_out_curr,-- Исходящие остатки (в валюте)
	bal_out_total)-- Исходящие остатки (итого)

	-- Подзапрос для всех столбцов, кроме исходящих остатков
	with
	q1 as (
		select
			date_trunc('month', cur_oper_date) as date_from,
			(date_trunc('month', cur_oper_date) + interval '1 month - 1 day') as date_to,
			mlas.chapter as chapter,
			substr(mad.account_number, 1, 5) as num_sc,
			case when mad.char_type = 'A' then 1 else 2 end as a_p,
			sum(case
	           		when mcd.currency_code in ('643', '810') then fbf.balance_out
	           		else 0
                end) as bal_in_rub,
          	sum(case
	          		when mcd.currency_code not in ('643', '810') then fbf.balance_out * merd.reduced_cource
	          		else 0
                end) as bal_in_curr,
          	sum(case
	          		when mcd.currency_code in ('643', '810') then fbf.balance_out
	          		else fbf.balance_out * merd.reduced_cource
	          	end) as bal_in_total,
			coalesce(sum(case
	           				when mcd.currency_code in ('643', '810') then datf.deb_amt_rub
	           				else 0
	           			end), 0) as turn_deb_rub,
			coalesce(sum(case
							when mcd.currency_code not in ('643', '810') then datf.deb_amt_rub
							else 0
						end), 0) as turn_deb_curr,
			coalesce(sum(datf.deb_amt_rub), 0) as turn_deb_total,
			coalesce(sum(case
	           				when mcd.currency_code in ('643', '810') then datf.cre_amt_rub
	           				else 0
	           			end), 0) as turn_cre_rub,
			coalesce(sum(case
	           				when mcd.currency_code not in ('643', '810') then datf.cre_amt_rub
							else 0
						end), 0) as turn_cre_curr,
			coalesce(sum(datf.cre_amt_rub), 0) as turn_cre_total

		from ds.md_ledger_account_s as mlas
		join ds.md_account_d as mad on substr(mad.account_number, 1, 5) = to_char(mlas.ledger_account, 'fm99999999')
		join ds.md_currency_d as mcd on mcd.currency_rk = mad.currency_rk
		left join ds.ft_balance_f as fbf on fbf.account_rk = mad.account_rk
			and fbf.on_date = (date_trunc('month', cur_oper_date) - interval '1 day')
		left join ds.md_exchange_rate_d as merd on merd.currency_rk = mad.currency_rk
			and cur_oper_date between merd.data_actual_date and merd.data_actual_end_date
		left join dm.dm_account_turnover_f as datf on datf.account_rk = mad.account_rk
			and datf.oper_date between date_trunc('month', cur_oper_date) and (date_trunc('month', cur_oper_date) + interval '1 month - 1 day')
		where
			cur_oper_date between mlas.start_date and mlas.end_date and
			cur_oper_date between mad.data_actual_date and mad.data_actual_end_date and
			cur_oper_date between mcd.data_actual_date and mcd.data_actual_end_date
		group by
    		mlas.chapter,
			substr(mad.account_number, 1, 5),
			a_p),
	-- Подзапрос для всех столбцов вместе с исходящими остатками
	q2 as (
		select *,
			(case
        		when a_p = 1 then bal_in_rub - turn_cre_rub + turn_deb_rub
        		when a_p = 2 then bal_in_rub + turn_cre_rub - turn_deb_rub
        		else 0
      	 	end) as bal_out_rub,
			(case
				when a_p = 1 then bal_in_curr - turn_cre_curr + turn_deb_curr
				when a_p = 2 then bal_in_curr + turn_cre_curr - turn_deb_curr
				else 0
		 	end) as bal_out_curr
 	  	from q1)

	-- Запрос из полученных таблиц для записи в целевую таблицу
	select *, bal_out_curr + bal_out_rub as bal_out_total
	from q2;
	end;
$procedure$