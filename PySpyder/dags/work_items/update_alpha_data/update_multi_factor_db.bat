@ECHO off
chcp 65001

matlab -nodesktop -r -wait update_multi_factor_db(%1)

set PATH=D:\Anaconda3\envs\py35;D:\Anaconda3\envs\py35\Scripts;D:\Anaconda3\envs\py35\Library\bin

python encodingTransform.py update_multi_factor_db.log
type update_multi_factor_db.log

@ECHO on