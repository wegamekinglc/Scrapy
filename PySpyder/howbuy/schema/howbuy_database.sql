create table HOWBUY_FUND_HOLDING
(
	howbuyCODE char(6) not null,
	fundName text null,
	publicationDate datetime not null,
	holdingAmount double null,
	holdingPercentage double null,
	changeAmount double null,
	instrumentID int not null,
	instrumentName text null,
	primary key (howbuyCODE, publicationDate, instrumentID)
);


create table HOWBUY_FUND_INDEX
(
	tradingDate datetime not null,
	howbuyCode char(6) not null,
	indexName text not null,
	indexLevel double not null,
	indexLevelChg double not null,
	adjustedHS300 double not null,
	primary key (tradingDate, howbuyCode)
);


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
);

create index HOWBUY_FUND_TYPE_setupDate_howbuyCODE_index
	on HOWBUY_FUND_TYPE (setupDate, howbuyCODE);


create table HOWBUY_STYLE_RET
(
	tradingDate datetime not null,
	howbuyStrategy text not null,
	max_ret double not null,
	min_ret double not null,
	median_ret double not null,
	mean_ret double not null,
	primary key (tradingDate, howbuyStrategy(10))
);