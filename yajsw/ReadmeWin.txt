Download yajsw-beta-10.9 or higher

modify runConsole.bat change line 1 to line 2
line1 : %wrapper_bat% -c %conf_file%
line2 : %wrapper_bat% -c %*

Open cmd (with admin rights for Vista/ win 7 or higher)

Cd <<path>>\yajsw-beta-10.9\bat
1. Get config

#run the component (quickcache) find out its PID (process id)

<<path>>\yajsw-beta-10.9\bat>genConfig <PID>
Sample  :<<path>>\yajsw-beta-10.9\bat>genConfig 6380

Open output file like <<path>>\yajsw-beta-10.9\bat\..\conf\wrapper.conf-nofile

Save file with new name say wrapper.conf in location <<path to wrapper.conf>>.


run (to test):
runConsole.bat "<<path to wrapper.conf>>\wrapper.conf"
Verify if component working – logs and app is working

installing:

modify installService.bat change line 1 to line 2
line1 : %wrapper_bat% -c %conf_file%
line2 : %wrapper_bat% -c %*

run:
installService.bat <<path to wrapper.conf>>\wrapper.conf"


testing:

modify startService.bat change line 1 to line 2
line1 : %wrapper_bat% -t %conf_file%
line2 : %wrapper_bat% -t %*
startService.bat <<path to wrapper.conf>>\wrapper.conf"

uninstallService.bat:

modify uninstallService.bat change line 1 to line 2
line1 : %wrapper_bat% -t %conf_file%
line2 : %wrapper_bat% -r %*

uninstallService.bat <<path to wrapper.conf>>\wrapper.conf"

