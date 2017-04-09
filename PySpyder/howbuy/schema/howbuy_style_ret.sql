create table hedge_fund.HOWBUY_STYLE_RET
(
	tradingDate datetime not null,
	howbuyStrategy text not null,
	max_ret double not null,
	min_ret double not null,
	median_ret double not null,
	mean_ret double not null,
	primary key (tradingDate, howbuyStrategy(6)),
	constraint HOWBUY_STYLE_RET_tradingDate_howbuyStrategy_uindex
		unique (tradingDate, howbuyStrategy(6))
)
;