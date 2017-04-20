@ECHO off
chcp 65001

set PATH=D:\Program Files\Tinysoft\Analyse.NET;D:\Program Files\MATLAB\R2016b\bin;D:\Program Files\MATLAB\R2016b\runtime\win64;D:\Anaconda3\envs\py35;D:\Anaconda3\envs\py35\Scripts;D:\Anaconda3\envs\py35\Library\bin;

matlab -nodesktop -r -wait update_alpha_500(%1)

python encodingTransform.py update_alpha_500.log
type update_alpha_500.log

@ECHO on