@ECHO off
chcp 65001
set PATH=C:\Program Files (x86)\Intel\iCLS Client\;C:\Program Files\Intel\iCLS Client\;%SystemRoot%\system32;%SystemRoot%;%SystemRoot%\System32\Wbem;%SYSTEMROOT%\System32\WindowsPowerShell\v1.0\;C:\Program Files (x86)\Intel\Intel(R) Management Engine Components\DAL;C:\Program Files\Intel\Intel(R) Management Engine Components\DAL;C:\Program Files (x86)\Intel\Intel(R) Management Engine Components\IPT;C:\Program Files\Intel\Intel(R) Management Engine Components\IPT;%USERPROFILE%\.dnx\bin;C:\Program Files\Microsoft DNX\Dnvm\;D:\Program Files\Tinysoft\Analyse.NET;D:\Anaconda3\envs\py35;D:\Anaconda3\envs\py35\Scripts;D:\Anaconda3\envs\py35\Library\bin

python D:\dev\svn\datacenter\trunk\daily_future.py %1

@ECHO on