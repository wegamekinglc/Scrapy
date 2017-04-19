@ECHO off
chcp 65001

matlab -nodesktop -r -wait update_non_alpha_data(%1)
type update_non_alpha_data.log

@ECHO on