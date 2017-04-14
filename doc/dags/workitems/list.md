# Work Items

# Daily Data Task Schedule
| finished?| Task Name | Code Repo | Program Language | Time | Time-consuming | Machine IP | Dependency Task | Executable on Linux |
| ---| --- | --- | --- |  --- | --- | --- | --- | --- |
| |updateMultiFactorDB | https://10.63.6.72/svn/lishun/Research/DataUpdate | Matlab | 16:00 | 10 min | 10.63.6.12 | None | N (TinySoft) |
| |updateOneDayNonAlphaData_PM | https://10.63.6.72/svn/lishun/Research/DataUpdate | Matlab | 16:10 | 10 min | 10.63.6.12 | updateMultiFactorDB | N (TinySoft) |
| |updateOneDayNonAlphaData_PM500 | https://10.63.6.72/svn/lishun/Research/DataUpdate500 | Matlab | 16:20 | 10 min | 10.63.6.12 | updateOneDayNonAlphaData_PM | N (TinySoft) |
| |updateOneDayAlphaData | https://10.63.6.72/svn/lic/MultiFactorTradingSystem | Matlab | 16:30 | 20 min | 10.63.6.12 | updateOneDayNonAlphaData_PM500 | N (TinySoft) |
| |datacenter_index_and_fund| https://10.63.6.72/svn/Lishun/Datacenter/trunk | Python | 16:30 | 10 min | 10.63.6.117 | None | N (TinySoft) |
| |datacenter_eqy| https://10.63.6.72/svn/Lishun/Datacenter/trunk | Python | 16:40 | 20 min | 10.63.6.117 | None | N (TinySoft) |
| |datacenter_futures| https://10.63.6.72/svn/Lishun/Datacenter/trunk | Python | 17:00 | 15 min | 10.63.6.117 | None | N (TinySoft) |
| √|DataChecker| https://10.63.6.72/svn/lic/DataCheck/branches/new_checker/DataChecker | Python | 17:15 | 35 min | 10.63.6.176 | datacenter_eqy & datacenter_futures | Y |
