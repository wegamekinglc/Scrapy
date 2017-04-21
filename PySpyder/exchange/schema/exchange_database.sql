create table suspend_info
(
	effectiveDate datetime not null,
	instrumentID char(6) not null,
	instrumentName text null,
	status char(2) null,
	reason text null,
	stopTime text null,
	primary key (effectiveDate, instrumentID),
	constraint suspend_info_effectiveDate_instrumentID_uindex
		unique (effectiveDate, instrumentID)
);


create table exchange.announcement_info
(
	reportDate datetime not null,
	instrumentID char(6) not null,
	title text not null,
	url text null,
	updateTime datetime not null,
	exchangePlace char(4) not null
)
;

create index announcement_info_reportDate_exchangePlace_index
	on announcement_info (reportDate, exchangePlace)
;



