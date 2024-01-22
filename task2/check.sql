truncate dm.dm_account_turnover_f;

do $$
	begin
		for i in 1..31 loop
			call dm.fill_dm_account_turnover_f(to_date(concat('2018-01-', i) , 'yyyy-mm-dd'));
		end loop;
	end;$$

--------------------------------------------------------------------------------------------------

truncate dm.dm_f101_round_f;

call dm.fill_f101_round_f(to_date('2018-01-01', 'yyyy-mm-dd'))