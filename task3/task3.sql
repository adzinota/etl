create table if not exists logs.logs_export (
	action_time timestamptz,
	action_type varchar
);



create table if not exists dm.dm_f101_round_f_v2 (
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