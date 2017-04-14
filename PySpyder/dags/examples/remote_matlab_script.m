delete('running.log');
diary('running.log')

diary on
s = 2 .* 2;
disp(['Current time is ', datestr(now, 31)]);
disp(['Value of s is ', sprintf('%f', s)]);
diary off

exit;
