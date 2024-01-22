create schema dm;

create table dm.dm_account_turnover_f (
	oper_date date not null,
	account_rk numeric not null,
	cre_amt_t_rub numeric(23,8) null,
	cre_amt_rub numeric(23,8) null,
	deb_amt_t_rub numeric(23,8) null,
	deb_amt_rub numeric(23,8) null
);

create table dm.dm_f101_round_f (
    date_from date,
    date_to date,
    chapter char(1),
    num_sc char(5),
	a_p numeric(23,8),
    bal_in_rub numeric(23,8),
    bal_in_curr numeric(23,8),
    bal_in_total numeric(23,8),
    turn_deb_rub numeric(23,8),
    turn_deb_curr numeric(23,8),
    turn_deb_total numeric(23,8),
    turn_cre_rub numeric(23,8),
    turn_cre_curr numeric(23,8),
    turn_cre_total numeric(23,8),
    bal_out_rub numeric(23,8),
    bal_out_curr numeric(23,8),
    bal_out_total numeric(23,8)
);