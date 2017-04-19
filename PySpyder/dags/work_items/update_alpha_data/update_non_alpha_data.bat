@ECHO off
chcp 65001

matlab -nodesktop -r -wait update_non_alpha_data(%1)

set PATH=D:\Anaconda3\envs\py35;D:\Anaconda3\envs\py35\Scripts;D:\Anaconda3\envs\py35\Library\bin

python encodingTransform.py update_non_alpha_data.log
type update_non_alpha_data.log

@ECHO on