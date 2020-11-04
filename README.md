##ExpressMaintenance

This is a fork off the CodePlex project ExpressMaint. The CodePlex project hasn't been updated in a few years so I've (re) forked the project and am releasing it to use NuGet SQLManagementObjects11 (SQLServer2013)

Also added a static copy of the original web page describing process and usage.

Original project - SQLDBAtips.com.  Code from  https://expressmaint.codeplex.com/

Binary files available in the 1-1 ZIP folder. extract to working folder.
my usage to ensure log files truncated.  

w:

cd\sqlbackups

expressmaint.exe -S (local) -D ALL_USER -T LOG -R w:\sqlbackups -RU WEEKS -RV 3 -B w:\sqlbackups -BU DAYS -BV 2  -V -C


