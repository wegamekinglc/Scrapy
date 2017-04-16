create table suspend_info
(
	effectiveDate datetime not null,
	instrumentID char(6) not null,
	instrumentName text null,
	status char(2) null,
	reason text null,
	primary key (effectiveDate, instrumentID),
	constraint suspend_info_effectiveDate_instrumentID_uindex
		unique (effectiveDate, instrumentID)
);

