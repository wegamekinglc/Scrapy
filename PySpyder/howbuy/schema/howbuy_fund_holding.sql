create table hedge_fund.HOWBUY_FUND_HOLDING
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
)
;

