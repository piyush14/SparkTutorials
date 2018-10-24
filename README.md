Running Spark applications on Windows in general is no different than running it on other operating systems like Linux or macOS.

What makes the huge difference between the operating systems is Hadoop that is used internally for file system access in Spark.

You may run into few minor issues when you are on Windows due to the way Hadoop works with Windows' POSIX-incompatible NTFS filesystem.

If you hit the below exception

    java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.

The steps to fix it:

    Download  winutils.exe binary from https://github.com/jleetutorial/sparkTutorial/blob/winutils/winutils.exe
    Save winutils.exe binary to a directory of your choice, e.g. c:\hadoop\bin
    
    Set the HADOOP_HOME  and PATH  environment in Control Panel so any Windows program would use them.
    (Instructions: https://www.computerhope.com/issues/ch000549.htm) 
        Set HADOOP_HOME to reflect the directory with winutils.exe (without bin).
        Set PATH environment variable to include %HADOOP_HOME%\bin as follows:

    set HADOOP_HOME=c:\hadoop
    set PATH=%HADOOP_HOME%\bin;%PATH%

# SparkTutorials
