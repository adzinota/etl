create or replace procedure dm.fill_dm_account_turnover_f(cur_oper_date date)
language plpgsql
as $procedure$
	begin
		-- Вставка полученных значений в целевую таблицу
		insert into dm.dm_account_turnover_f
		(oper_date,
		account_rk,
		cre_amt_t_rub,
		cre_amt_rub,
		deb_amt_t_rub,
		deb_amt_rub)
		-- Подзапросы для получения таблиц по кредиту и дебету
    	with
    	credit as
    		(select
    			fpf.credit_account_rk as account_rk,
				fpf.credit_amount* coalesce(merd.reduced_cource, 1) as cre_amt_rub,
				cast(null as numeric) as deb_amt_t_rub,
				cast(null as numeric) as deb_amt_rub
			from ds.ft_posting_f as fpf
			left join ds.md_account_d as mad on fpf.credit_account_rk = mad.account_rk
				and cur_oper_date between mad.data_actual_date and mad.data_actual_end_date
			left join ds.md_exchange_rate_d as merd on mad.currency_rk = merd.currency_rk
				and cur_oper_date between merd.data_actual_date and merd.data_actual_end_date
			where
				fpf.oper_date = cur_oper_date),
		debet as
			(select
				fpf.debet_account_rk as account_rk,
				cast(null as numeric) as cre_amt_t_rub,
				cast(null as numeric) as cre_amt_rub,
				fpf.debet_amount * coalesce(merd.reduced_cource, 1) as deb_amt_rub
			from ds.ft_posting_f as fpf
			left join ds.md_account_d as mad on fpf.debet_account_rk = mad.account_rk
				and cur_oper_date between mad.data_actual_date and mad.data_actual_end_date
			left join ds.md_exchange_rate_d as merd on mad.currency_rk = merd.currency_rk
				and cur_oper_date between merd.data_actual_date and merd.data_actual_end_date
			where
				fpf.oper_date = cur_oper_date)
		-- Запрос из полученных таблиц для записи в целевую таблицу
    	select
			cur_oper_date,
			account_rk,
			round((sum(cre_amt_rub)/1000)::numeric, 1),
			round(sum(cre_amt_rub)::numeric,2),
			round((sum(deb_amt_rub)/1000)::numeric, 1),
			round(sum(deb_amt_rub)::numeric,2)
		from (select * from credit	union all select * from debet)
		group by account_rk
		order by account_rk;
	end;
$procedure$