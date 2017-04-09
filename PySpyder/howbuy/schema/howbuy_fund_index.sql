create table HOWBUY_FUND_INDEX
(
	tradingDate datetime not null,
	howbuyCode char(6) not null,
	indexName text not null,
	indexLevel double not null,
	indexLevelChg double not null,
	adjustedHS300 double not null,
	primary key (tradingDate, howbuyCode),
	constraint HOWBUY_FUND_INDEX_tradingDate_howbuyCode_uindex
		unique (tradingDate, howbuyCode)
)
;