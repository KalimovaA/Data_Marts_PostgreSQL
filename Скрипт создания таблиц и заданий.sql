--drop table DM.DM_ACCOUNT_TURNOVER_F;
create table DM.DM_ACCOUNT_TURNOVER_F (
	on_date				date,
	account_rk			numeric,
	credit_amount			numeric (23,8),
	credit_amount_rub		numeric (23,8),
	debet_amount			numeric (23,8),
	debet_amount_rub		numeric (23,8)
);

select * from DM.DM_ACCOUNT_TURNOVER_F order by account_rk ;

--drop table DM.DM_F101_ROUND_F;
create table DM.DM_F101_ROUND_F (
	FROM_DATE		date,
	TO_DATE			date,
	CHAPTER			char (1),
	LEDGER_ACCOUNT		char (5),
	CHARACTERISTIC		char (1),
	BALANCE_IN_RUB		numeric (23,8),
	R_BALANCE_IN_RUB	numeric (23,8),
	BALANCE_IN_VAL		numeric (23,8),
	R_BALANCE_IN_VAL	numeric (23,8),
	BALANCE_IN_TOTAL	numeric (23,8),
	R_BALANCE_IN_TOTAL	numeric (23,8),
	TURN_DEB_RUB		numeric (23,8),
	R_TURN_DEB_RUB		numeric (23,8),
	TURN_DEB_VAL		numeric (23,8),
	R_TURN_DEB_VAL		numeric (23,8),
	TURN_DEB_TOTAL		numeric (23,8),
	R_TURN_DEB_TOTAL	numeric (23,8),
	TURN_CRE_RUB		numeric (23,8),
	R_TURN_CRE_RUB		numeric (23,8),
	TURN_CRE_VAL		numeric (23,8),
	R_TURN_CRE_VAL		numeric (23,8),
	TURN_CRE_TOTAL		numeric (23,8),
	R_TURN_CRE_TOTAL	numeric (23,8),
	BALANCE_OUT_RUB		numeric (23,8),
	R_BALANCE_OUT_RUB	numeric (23,8),
	BALANCE_OUT_VAL		numeric (23,8),
	R_BALANCE_OUT_VAL	numeric (23,8),
	BALANCE_OUT_TOTAL	numeric (23,8),
	R_BALANCE_OUT_TOTAL	numeric (23,8)
);

select * from dm.dm_f101_round_f order by ledger_account;

--drop table DM.LG_MESSAGES;
create table DM.LG_MESSAGES ( 	
	record_id		int not null,
	date_time		timestamp not null,		
	pid			int not null,
	message			varchar not null,
	message_type		int not null,
	usename			varchar, 
	datname			varchar, 
	client_addr		varchar,
	application_name	varchar,
	backend_start		timestamp,
	constraint lg_messages_pk primary key (record_id)
);

select * from DM.LG_MESSAGES;

-- Создание последовательности для логов
create sequence dm.seq_lg_messages start 1;

-- Создание задания на вызов процедуры оборотов за январь 2018г.
DO $uniq_tAg$
DECLARE
    jid integer;
    scid integer;
BEGIN
-- Creating a new job
INSERT INTO pgagent.pga_job(
    jobjclid, jobname, jobenabled
) VALUES (
    1, 'fill_account_turnover', true
) RETURNING jobid INTO jid;
-- Inserting a step
INSERT INTO pgagent.pga_jobstep (
    jstjobid, jstname, jstenabled, jstkind,
    jstdbname, jstonerror,
    jstcode
) VALUES (
    jid, 'fill_account_turnover_1', true, 's',
    'postgres', 'f',
    'do $$
	declare
		enter_date	date := ''2018-01-10'';
		first_day	date := date_trunc(''month'', enter_date);
		last_day	int  := extract (day from date_trunc(''month'', enter_date) + INTERVAL ''1 MONTH - 1 day'');
	begin
		for i in 0..last_day-1 
		loop
			call dm.fill_account_turnover_f (i_OnDate => first_day + i);
		end loop;
	end $$;
	'
	);
-- Inserting a schedule
INSERT INTO pgagent.pga_schedule(
    jscjobid, jscname, jscenabled,
    jscstart, 
    jscminutes, jschours, jscweekdays, jscmonthdays, jscmonths
) VALUES (
    jid, 'fill_sched', true,
    '2022-01-18 17:04:14', 
    -- Minutes
    ARRAY[true,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false]::boolean[],
    -- Hours
    ARRAY[true,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false]::boolean[],
    -- Week days
    ARRAY[true,false,false,false,false,false,false]::boolean[],
    -- Month days
    ARRAY[false,true,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false]::boolean[],
    -- Months
    ARRAY[false,true,false,false,false,false,false,false,false,false,false,false]::boolean[]
) RETURNING jscid INTO scid;
END
$uniq_tAg$;

-- Создание задания на вызов процедуры 101-й формы
DO $uniq_tAg$
DECLARE
    jid integer;
    scid integer;
BEGIN
-- Creating a new job
INSERT INTO pgagent.pga_job(
    jobjclid, jobname, jobenabled
) VALUES (
    1, 'fill_f101', true
) RETURNING jobid INTO jid;
-- Inserting a step
INSERT INTO pgagent.pga_jobstep (
    jstjobid, jstname, jstenabled, jstkind,
    jstdbname, jstonerror,
    jstcode
) VALUES (
    jid, 'fill_f101', true, 's',
    'postgres', 'f',
    'call dm.fill_f101_round_f (''2018-01-31'')
	'
	);
-- Inserting a schedule
INSERT INTO pgagent.pga_schedule(
    jscjobid, jscname, jscenabled,
    jscstart, jscminutes, jschours, jscweekdays, jscmonthdays, jscmonths
) VALUES (
    jid, 'fill_sched', true,
    '2022-01-18 17:07:14', 
    -- Minutes
    ARRAY[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false]::boolean[],
    -- Hours
    ARRAY[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false]::boolean[],
    -- Week days
    ARRAY[false,false,false,false,false,false,false]::boolean[],
    -- Month days
    ARRAY[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false]::boolean[],
    -- Months
    ARRAY[false,false,false,false,false,false,false,false,false,false,false,false]::boolean[]
) RETURNING jscid INTO scid;
END
$uniq_tAg$;

call dm.fill_f101_round_f ('2018-01-31');

-- Просмотр отработавших заданий
select * from pgagent.pga_joblog
order by 1 desc;
