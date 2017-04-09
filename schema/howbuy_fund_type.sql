create table HOWBUY_FUND_TYPE
(
	howbuyCODE char(6) not null
		primary key,
	fundName text not null,
	fundManagementComp text not null,
	manager text not null,
	setupDate datetime not null,
	howbuyStrategy text not null,
	adjPrice double,
	priceDate datetime
)
;

create index HOWBUY_FUND_TYPE_setupDate_howbuyCODE_index
	on HOWBUY_FUND_TYPE (setupDate, howbuyCODE)
;
