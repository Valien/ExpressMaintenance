Option Explicit On
Option Strict On

#Region "Imports"

Imports Microsoft.SqlServer.Management.Common
Imports Microsoft.SqlServer.Management.Smo
Imports Microsoft.SqlServer
Imports System.IO
Imports System.Xml
Imports System.Xml.Schema
Imports System.Data
Imports System.Data.SqlClient
Imports System.Text
Imports System.Collections.Specialized

#End Region

Module ExpressMaint

#Region "Globals and Constants"


    Const ALL As String = "ALL"
    Const ALL_SYSTEM As String = "ALL_SYSTEM"
    Const ALL_USER As String = "ALL_USER"
    Const DEFAULT_TIMEOUT As Integer = 10           'Default statement timeout

    Dim querytimeout As Integer = 0                 'Statement timeout
    Dim srv As Server                               'DMO Server
    Dim conn As ServerConnection                    'DMO Connection
    Dim paramfile As String                         'XML parameter file
    Dim pServer As String                           'SQL Server name
    Dim pDatabase As String                         'Database parameter
    Dim pOptype As String                           'Operation Type parameter
    Dim pBackupFolder As String                     'Backup folder
    Dim pDBRetainunit As String                     'Backup retention unit
    Dim pDBRetainvalue As Integer = -1              'Backup retention value
    Dim pReportFolder As String                     'Report folder
    Dim pRPTRetainunit As String                    'Report retention unit
    Dim pRPTRetainvalue As Integer = -1             'Report retention value
    Dim bCheckarchive As Boolean = False            'Check archive bit on backups ?
    Dim dt As DataTable                             'Holds database info
    Dim rfile As String                             'Report file
    Dim bReport As Boolean = False                  'Are we producing a report ?
    Dim bVerify As Boolean = False                  'Do we want to verify backups ?
    Dim stage As Integer = 1                        'Current Stage
    Dim debugargs As Boolean = False                'Debug switch for commandline arguments
    Dim globalstart As DateTime = Now()             'Start time of job
    Dim bLogAttemptOnSimple As Boolean = False      'Catch attempts to do a log backup of simple database
    Dim delcount As Integer = 0                     'Count of deleted files
    Dim bTimeStampFirst As Boolean = False          'Add timestamp to the start of the filename
    Dim bContinueOnError As Boolean = False         'Continue multi database operation if error encountered
    Dim bMultiDBError As Boolean = False            'Indicate if an error was encountered in a multi database operation
    Dim filelist As ArrayList = New ArrayList()     'Holds filenames of files generated in this execution
    Dim sqllogin As String                          'SQL Login
    Dim sqlpass As String                           'SQL Password
    Dim bIsTrustedConn As Boolean = True            'Are we using a trusted connection
    Dim bCustomFilename As Boolean = False          'Are we using a custom backup filename
    Dim customfilename As String                    'Custom backup filename format

    Dim currentDomain As AppDomain = AppDomain.CurrentDomain
    Dim Version As String = My.Application.Info.Version.ToString()
    Dim Usage As String = vbCrLf & "ExpressMaint utility v" & Version & vbCrLf & _
                        "Created by Jasper Smith (www.sqldbatips.com)" & vbCrLf & _
                        " " & vbCrLf & _
                        "Usage:" & vbCrLf & _
                        "[-?] " & vbTab & "Show help" & vbCrLf & _
                        "[-S] " & vbTab & "SQL Server Name" & vbCrLf & _
                        "[-U] " & vbTab & "SQL Server Login" & vbCrLf & _
                        "[-P] " & vbTab & "SQL Server Password" & vbCrLf & _
                        "[-D] " & vbTab & "Database Name or " & ALL_USER & "," & ALL_SYSTEM & "," & ALL & vbCrLf & _
                        "[-T] " & vbTab & "Type of operation. Can be DB,DIF,LOG,CHECKDB,REORG,REINDEX,STATS,STATSFULL" & vbCrLf & _
                        "[-B] " & vbTab & "Base backup folder path" & vbCrLf & _
                        "[-R] " & vbTab & "Report folder path" & vbCrLf & _
                        "[-BU]" & vbTab & "Backup retention time unit. Can be MINUTES,HOURS,DAYS,WEEKS" & vbCrLf & _
                        "[-BV]" & vbTab & "Backup retention value" & vbCrLf & _
                        "[-RU]" & vbTab & "Report retention time unit. Can be MINUTES,HOURS,DAYS,WEEKS" & vbCrLf & _
                        "[-RV]" & vbTab & "Report retention value" & vbCrLf & _
                        "[-V] " & vbTab & "Verify backup" & vbCrLf & _
                        "[-A] " & vbTab & "Check archive attribute on file before deleting" & vbCrLf & _
                        "[-DS]" & vbTab & "Add the timestamp for files at the start of the filename" & vbCrLf & _
                        "[-TO]" & vbTab & "Query Timeout in minutes (Default:10)" & vbCrLf & _
                        "[-C] " & vbTab & "Continue multiple database operation if one or more fails" & vbCrLf & _
                        "[-BF]" & vbTab & "Custom Backup filename format" & vbCrLf

    Dim dbquery As String = "select distinct d.name,case when d.database_id < 5 then 1 else 0 end as IsSystem,d.recovery_model " & _
                        "from sys.databases as d " & _
                        "join sys.master_files as f on d.database_id = f.database_id " & _
                        "where d.state_desc = N'ONLINE' " & _
                        "and d.source_database_id is null " & _
                        "and d.name not like f.physical_name"

#End Region

    'Main
    Sub Main()
        AddHandler currentDomain.UnhandledException, AddressOf MyHandler

        'Get Command Line Arguments
        Dim args As String() = Environment.GetCommandLineArgs()
        If args Is Nothing OrElse args.Length < 3 Then
            Console.Write(Usage)
            Exit Sub
        Else
            If ParseArguments(args) = False Then
                Console.WriteLine("")
                Console.Write(Usage)
                Exit Sub
            End If
        End If

        If bReport Then
            Try
                GenerateReportFilename(pReportFolder, pDatabase)
                filelist.Add(rfile)
                WriteToFile("", rfile)
            Catch ex As IOException
                Console.WriteLine("Error initialising log file")
                Console.WriteLine(ex.Message)
                If Not ex.InnerException Is Nothing Then
                    Console.WriteLine(ex.InnerException.Message)
                End If
                Environment.ExitCode = -2
                Exit Sub
            End Try

        End If

        Try
            srv = New Server(pServer)
            conn = srv.ConnectionContext
            If bIsTrustedConn Then
                conn.LoginSecure = True
            Else
                conn.LoginSecure = False
                conn.Login = sqllogin
                conn.Password = sqlpass
            End If

            conn.ApplicationName = "Expressmaint " & Version

            If querytimeout < 1 Then
                querytimeout = DEFAULT_TIMEOUT
            End If

            conn.StatementTimeout = (querytimeout * 60)

            'Limit the properties 
            srv.SetDefaultInitFields(GetType(Database), _
                New String() {"Name", "Status", "IsSystemObject"})
            srv.SetDefaultInitFields(GetType(DatabaseOptions), _
                New String() {"RecoveryModel", "AutoClose"})
            srv.SetDefaultInitFields(GetType(Table), _
                New String() {"Name", "HasIndex"})
            srv.SetDefaultInitFields(GetType(Index), _
                New String() {"Name", "IsDisabled"})
            srv.SetDefaultInitFields(GetType(Column), _
                New String() {"ID", "Name"})

            If bReport Then
                WriteToFile(vbCrLf & "Expressmaint utility v" & Version & " , Logged on to SQL Server [" & srv.Name & "] as [" & srv.ConnectionContext.TrueLogin & "]" & vbCrLf & "Created by Jasper Smith (www.sqldbatips.com)" & vbCrLf & vbCrLf, rfile)

                If debugargs Then
                    WriteToFile("Debug Mode Enabled " & vbCrLf, rfile)
                End If

                Select Case pOptype
                    Case "DB", "DIF", "LOG"
                        WriteToFile("Starting backup on " & Now.ToString & vbCrLf & vbCrLf, rfile)
                    Case "CHECKDB"
                        WriteToFile("Starting CheckDB on " & Now.ToString & vbCrLf & vbCrLf, rfile)
                    Case "REINDEX", "REORG"
                        WriteToFile("Starting Reindex on " & Now.ToString & vbCrLf & vbCrLf, rfile)
                    Case "STATS", "STATSFULL"
                        WriteToFile("Starting Statistics Update on " & Now.ToString & vbCrLf & vbCrLf, rfile)
                End Select
            End If

        Catch ex As SmoException
            ShowError(ex)
            If conn.IsOpen Then
                conn.Disconnect()
            End If
            If Not (srv Is Nothing) Then
                srv = Nothing
            End If
            If bReport Then
                WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-3)" & vbCrLf, rfile)
            End If
            Environment.ExitCode = -3
            Exit Sub
        Catch ex As Exception
            ShowError(ex)
            If conn.IsOpen Then
                conn.Disconnect()
            End If
            If Not (srv Is Nothing) Then
                srv = Nothing
            End If
            If bReport Then
                WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-3)" & vbCrLf, rfile)
            End If
            Environment.ExitCode = -3
            Exit Sub
        End Try


        'Get and poulate the database list
        Try
            Makedt()
        Catch ex As Exception
            ShowError(ex)
            If conn.IsOpen Then
                conn.Disconnect()
            End If
            If Not (srv Is Nothing) Then
                srv = Nothing
            End If
            If bReport Then
                WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-4)" & vbCrLf, rfile)
            End If
            Environment.ExitCode = -4
            Exit Sub
        End Try


        If pOptype = "DB" Or pOptype = "DIF" Or pOptype = "LOG" Then
            Try
                DoBackup()
                If bLogAttemptOnSimple Then
                    ShowMessage(vbCrLf & Usage)
                    If conn.IsOpen Then
                        conn.Disconnect()
                    End If
                    If Not (srv Is Nothing) Then
                        srv = Nothing
                    End If
                    If bReport Then
                        WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-5)" & vbCrLf, rfile)
                    End If
                    Environment.ExitCode = -5
                    Exit Sub
                End If
                If bReport Then
                    WriteToFile(vbCrLf & "[" & CStr(stage) & "] Delete old Report files..." & vbCrLf, rfile)
                    DeleteOldFiles(rfile, "REPORT", pDatabase)
                End If
            Catch ex As SmoException
                If Not ex.InnerException Is Nothing Then
                    ShowError(ex.InnerException)
                End If
                ShowError(ex)
                If conn.IsOpen Then
                    conn.Disconnect()
                End If
                If Not (srv Is Nothing) Then
                    srv = Nothing
                End If
                If bReport Then
                    WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-5)" & vbCrLf, rfile)
                End If
                Environment.ExitCode = -5
                Exit Sub
            End Try
        End If

        If pOptype = "CHECKDB" Then
            Try
                DoCheckDB()
                If bReport Then
                    WriteToFile(vbCrLf & vbCrLf & "[" & CStr(stage) & "] Delete old Report files..." & vbCrLf, rfile)
                    DeleteOldFiles(rfile, "REPORT", pDatabase)
                End If
            Catch ex As Exception
                ShowError(ex)
                If conn.IsOpen Then
                    conn.Disconnect()
                End If
                If Not (srv Is Nothing) Then
                    srv = Nothing
                End If
                If bReport Then
                    WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-6)" & vbCrLf, rfile)
                End If
                Environment.ExitCode = -6
                Exit Sub
            End Try
        End If

        If pOptype = "REINDEX" Or pOptype = "REORG" Then
            Try
                DoReindex()
                If bReport Then
                    WriteToFile(vbCrLf & vbCrLf & "[" & CStr(stage) & "] Delete old Report files..." & vbCrLf, rfile)
                    DeleteOldFiles(rfile, "REPORT", pDatabase)
                End If
            Catch ex As Exception
                ShowError(ex)
                If conn.IsOpen Then
                    conn.Disconnect()
                End If
                If Not (srv Is Nothing) Then
                    srv = Nothing
                End If
                If bReport Then
                    WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-7)" & vbCrLf, rfile)
                End If
                Environment.ExitCode = -7
                Exit Sub
            End Try
        End If

        If pOptype = "STATS" Or pOptype = "STATSFULL" Then
            Try
                DoUpdateStats()
                If bReport Then
                    WriteToFile(vbCrLf & vbCrLf & "[" & CStr(stage) & "] Delete old Report files..." & vbCrLf, rfile)
                    DeleteOldFiles(rfile, "REPORT", pDatabase)
                End If
            Catch ex As Exception
                ShowError(ex)
                If conn.IsOpen Then
                    conn.Disconnect()
                End If
                If Not (srv Is Nothing) Then
                    srv = Nothing
                End If
                If bReport Then
                    WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-8)" & vbCrLf, rfile)
                End If
                Environment.ExitCode = -8
                Exit Sub
            End Try
        End If

        If conn.IsOpen Then
            conn.Disconnect()
        End If
        srv = Nothing

        If bContinueOnError And bMultiDBError Then
            If bReport Then
                WriteToFile(vbCrLf & vbCrLf & "Expressmaint processed all requested databases however one or more operations failed (see above)" & vbCrLf, rfile)
                WriteToFile("Expressmaint finished unsuccessfully at " & Now.ToString & " with Return Code(-9)" & vbCrLf, rfile)
            End If
            Environment.ExitCode = -9
        Else
            If bReport Then
                WriteToFile(vbCrLf & vbCrLf & "Expressmaint finished successfully at " & Now.ToString & " with Return Code(0)" & vbCrLf, rfile)
            End If
            Environment.ExitCode = 0
        End If

        Exit Sub
    End Sub

    'Check for disabled clustered index (i.e. disabled table)
    Private Function HasDisabledClusteredIndex(ByVal t As Table) As Boolean

        If t.HasClusteredIndex = False Then
            Return False
        End If

        For Each ind As Index In t.Indexes
            If ind.IsClustered And ind.IsDisabled Then
                Return True
            End If
        Next

        Return False

    End Function

    'DataTable for storing databases
    Private Sub Makedt()
        dt = New DataTable("Databases")
        Dim c As DataColumn
        c = New DataColumn("Name", GetType(System.String))
        dt.Columns.Add(c)
        c = New DataColumn("Status", GetType(System.Int32))
        dt.Columns.Add(c)
        c = New DataColumn("IsSystem", GetType(System.Boolean))
        dt.Columns.Add(c)
        c = New DataColumn("Recovery", GetType(System.Int32))
        dt.Columns.Add(c)
        Dim PrimaryKeyColumns(0) As DataColumn
        PrimaryKeyColumns(0) = dt.Columns("Name")
        dt.PrimaryKey = PrimaryKeyColumns
        Populatedt()
    End Sub

    'Populate DataTable
    Private Sub Populatedt()
        Dim d As DataRow
        Dim oConn As SqlConnection = New SqlConnection(srv.ConnectionContext.ConnectionString)
        oConn.Open()

        'Switched to using TSQL as SMO won't always return all the databases on Express (not started db)
        Dim oCmd As SqlCommand = New SqlCommand(dbquery)
        oCmd.CommandType = CommandType.Text
        oCmd.Connection = oConn
        Dim rdr As SqlDataReader

        Try
            rdr = oCmd.ExecuteReader()

            While rdr.Read()
                d = dt.NewRow
                d("Name") = rdr(0)
                d("Status") = 1
                d("IsSystem") = rdr(1)
                d("Recovery") = rdr(2)
                dt.Rows.Add(d)
            End While

        Catch ex As SqlException
            ShowError(ex)
        Catch ex As Exception
            ShowError(ex)
        Finally
            rdr.Close()
            oCmd.Dispose()
            oConn.Close()
            oConn.Dispose()
        End Try

        Dim myDataRowCollection As DataRowCollection
        Dim foundRow As DataRow
        myDataRowCollection = dt.Rows

        If myDataRowCollection.Contains("tempdb") Then
            foundRow = myDataRowCollection.Find("tempdb")
            myDataRowCollection.Remove(foundRow)
        End If
        If pDatabase.ToUpper <> "ALL_SYSTEM" And pDatabase.ToUpper <> "ALL_USER" And pDatabase.ToUpper <> "ALL" Then
            If Not myDataRowCollection.Contains(pDatabase) Then
                Throw New Exception("Database " & pDatabase & " not found" & vbCrLf)
            End If
        End If
    End Sub

    'Do the backups
    Private Sub DoBackup()
        Dim foundRows() As DataRow
        Dim fname As String
        Dim start As DateTime
        Dim finish As DateTime
        Dim bInError As Boolean

        If pDatabase = "ALL_SYSTEM" Then
            If pOptype = "DIF" Then
                Dim myDataRowCollection As DataRowCollection
                Dim foundRow As DataRow
                myDataRowCollection = dt.Rows
                If myDataRowCollection.Contains("master") Then
                    foundRow = myDataRowCollection.Find("master")
                    myDataRowCollection.Remove(foundRow)
                End If
            End If
            If pOptype = "LOG" Then
                foundRows = dt.Select("IsSystem = True and Status = 1 and Recovery <> 3", "Name ASC")
            Else
                foundRows = dt.Select("IsSystem = True and Status = 1", "Name ASC")
            End If
            For i As Integer = 0 To foundRows.GetUpperBound(0)
                If ((CInt(foundRows(i)(1)) And DatabaseStatus.Normal) = DatabaseStatus.Normal) Then
                    start = Now()
                    bInError = False

                    If bReport Then
                        WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Backup starting at " & Now.ToString & vbCrLf, rfile)
                    End If
                    Dim bk As Backup = New Backup
                    SetBackupOptions(bk)
                    bk.Database = foundRows(i)(0).ToString
                    fname = GenerateFilename(pBackupFolder, foundRows(i)(0).ToString, pOptype, bCustomFilename)
                    If fname = "-1" Then
                        Exit Sub
                    Else
                        Dim bki As New BackupDeviceItem(fname, DeviceType.File)
                        bk.Devices.Add(bki)
                        filelist.Add(fname)
                    End If
                    bk.Initialize = True

                    Try
                        bk.SqlBackup(srv)

                        If bReport Then
                            finish = Now()
                            If pOptype = "LOG" Then
                                WriteToFile("    Log backed up to " & fname & vbCrLf, rfile)
                            Else
                                WriteToFile("    Database backed up to " & fname & vbCrLf, rfile)
                            End If
                            WriteToFile("    Backup completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                        End If
                    Catch ex As SmoException
                        If Not bContinueOnError Then
                            Throw ex
                        Else
                            bInError = True
                            bMultiDBError = True
                            ShowError(ex)
                        End If
                    Catch ex As Exception
                        If Not bContinueOnError Then
                            Throw ex
                        Else
                            bInError = True
                            bMultiDBError = True
                            ShowError(ex)
                        End If
                    Finally
                        bk = Nothing
                    End Try

                    stage += 1

                    If bVerify And bInError = False Then
                        start = Now()
                        If bReport Then
                            WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Verify starting at " & Now.ToString & vbCrLf, rfile)
                        End If
                        Dim verifyerror As String = ""
                        Dim bIsValid As Boolean = False
                        Dim res As Restore = New Restore
                        Dim rsi As New BackupDeviceItem(fname, DeviceType.File)
                        res.Devices.Add(rsi)
                        SetVerifyOptions(res)
                        bIsValid = res.SqlVerify(srv, verifyerror)
                        If bIsValid Then
                            If bReport Then
                                finish = Now()
                                WriteToFile("    Backup file " & fname & " verified" & vbCrLf, rfile)
                                WriteToFile("    Verify backup completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                            End If
                        Else
                            WriteToFile("    Backup file " & fname & " failed verification" & vbCrLf, rfile)
                            WriteToFile("    " & verifyerror & vbCrLf & vbCrLf, rfile)
                        End If
                        stage += 1
                    End If

                    If bInError = False Then
                        WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Delete old backup files" & vbCrLf, rfile)
                        DeleteOldFiles(fname, pOptype, foundRows(i)(0).ToString)
                        stage += 1
                    End If
                End If
            Next i
        ElseIf pDatabase = "ALL_USER" Then
            If pOptype = "LOG" Then
                foundRows = dt.Select("IsSystem = False  and Status = 1 And Recovery <> 3", "Name ASC")
            Else
                foundRows = dt.Select("IsSystem = False  and Status = 1", "Name ASC")
            End If
            For i As Integer = 0 To foundRows.GetUpperBound(0)
                If ((CInt(foundRows(i)(1)) And DatabaseStatus.Normal) = DatabaseStatus.Normal) Then

                    start = Now()
                    bInError = False

                    If bReport Then
                        WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Backup starting at " & Now.ToString & vbCrLf, rfile)
                    End If
                    Dim bk As Backup = New Backup
                    SetBackupOptions(bk)
                    bk.Database = foundRows(i)(0).ToString
                    fname = GenerateFilename(pBackupFolder, foundRows(i)(0).ToString, pOptype, bCustomFilename)
                    If fname = "-1" Then
                        Exit Sub
                    Else
                        Dim bki As New BackupDeviceItem(fname, DeviceType.File)
                        bk.Devices.Add(bki)
                        filelist.Add(fname)
                    End If
                    bk.Initialize = True
                    Try
                        bk.SqlBackup(srv)

                        If bReport Then
                            finish = Now()
                            If pOptype = "LOG" Then
                                WriteToFile("    Log backed up to " & fname & vbCrLf, rfile)
                            Else
                                WriteToFile("    Database backed up to " & fname & vbCrLf, rfile)
                            End If
                            WriteToFile("    Backup completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                        End If
                    Catch ex As SmoException
                        If Not bContinueOnError Then
                            Throw ex
                        Else
                            bInError = True
                            bMultiDBError = True
                            ShowError(ex)
                        End If
                    Catch ex As Exception
                        If Not bContinueOnError Then
                            Throw ex
                        Else
                            bInError = True
                            bMultiDBError = True
                            ShowError(ex)
                        End If
                    Finally
                        bk = Nothing
                    End Try

                    stage += 1

                    If bVerify And bInError = False Then
                        start = Now()
                        If bReport Then
                            WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Verify starting at " & Now.ToString & vbCrLf, rfile)
                        End If
                        Dim verifyerror As String = ""
                        Dim bIsValid As Boolean = False
                        Dim res As Restore = New Restore
                        Dim rsi As New BackupDeviceItem(fname, DeviceType.File)
                        res.Devices.Add(rsi)
                        SetVerifyOptions(res)
                        bIsValid = res.SqlVerify(srv, verifyerror)
                        If bIsValid Then
                            If bReport Then
                                finish = Now()
                                WriteToFile("    Backup file " & fname & " verified" & vbCrLf, rfile)
                                WriteToFile("    Verify backup completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                            End If
                        Else
                            WriteToFile("    Backup file " & fname & " failed verification" & vbCrLf, rfile)
                            WriteToFile("    " & verifyerror & vbCrLf & vbCrLf, rfile)
                        End If
                        stage += 1
                    End If

                    If bInError = False Then
                        WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Delete old backup files" & vbCrLf, rfile)
                        DeleteOldFiles(fname, pOptype, foundRows(i)(0).ToString)
                        stage += 1
                    End If
                End If
            Next i
        ElseIf pDatabase = "ALL" Then
            If pOptype = "DIF" Then
                Dim myDataRowCollection As DataRowCollection
                Dim foundRow As DataRow
                myDataRowCollection = dt.Rows
                If myDataRowCollection.Contains("master") Then
                    foundRow = myDataRowCollection.Find("master")
                    myDataRowCollection.Remove(foundRow)
                End If
            End If
            If pOptype = "LOG" Then
                foundRows = dt.Select("Recovery <> 3 and Status = 1", "Name ASC")
            Else
                foundRows = dt.Select("Status = 1", "Name ASC")
            End If
            For i As Integer = 0 To foundRows.GetUpperBound(0)
                If ((CInt(foundRows(i)(1)) And DatabaseStatus.Normal) = DatabaseStatus.Normal) Then

                    start = Now()
                    bInError = False

                    If bReport Then
                        WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Backup starting at " & Now.ToString & vbCrLf, rfile)
                    End If
                    Dim bk As Backup = New Backup
                    SetBackupOptions(bk)
                    bk.Database = foundRows(i)(0).ToString
                    fname = GenerateFilename(pBackupFolder, foundRows(i)(0).ToString, pOptype, bCustomFilename)
                    If fname = "-1" Then
                        Exit Sub
                    Else
                        Dim bki As New BackupDeviceItem(fname, DeviceType.File)
                        bk.Devices.Add(bki)
                        filelist.Add(fname)
                    End If
                    bk.Initialize = True

                    Try
                        bk.SqlBackup(srv)

                        If bReport Then
                            finish = Now()
                            If pOptype = "LOG" Then
                                WriteToFile("    Log backed up to " & fname & vbCrLf, rfile)
                            Else
                                WriteToFile("    Database backed up to " & fname & vbCrLf, rfile)
                            End If
                            WriteToFile("    Backup completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                        End If
                    Catch ex As SmoException
                        If Not bContinueOnError Then
                            Throw ex
                        Else
                            bInError = True
                            bMultiDBError = True
                            ShowError(ex)
                        End If
                    Catch ex As Exception
                        If Not bContinueOnError Then
                            Throw ex
                        Else
                            bInError = True
                            bMultiDBError = True
                            ShowError(ex)
                        End If
                    Finally
                        bk = Nothing
                    End Try

                    stage += 1

                    If bVerify And bInError = False Then
                        start = Now()
                        If bReport Then
                            WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Verify starting at " & Now.ToString & vbCrLf, rfile)
                        End If
                        Dim verifyerror As String = ""
                        Dim bIsValid As Boolean = False
                        Dim res As Restore = New Restore
                        Dim rsi As New BackupDeviceItem(fname, DeviceType.File)
                        res.Devices.Add(rsi)
                        SetVerifyOptions(res)
                        bIsValid = res.SqlVerify(srv, verifyerror)
                        If bIsValid Then
                            If bReport Then
                                finish = Now()
                                WriteToFile("    Backup file " & fname & " verified" & vbCrLf, rfile)
                                WriteToFile("    Verify backup completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                            End If
                        Else
                            WriteToFile("    Backup file " & fname & " failed verification" & vbCrLf, rfile)
                            WriteToFile("    " & verifyerror & vbCrLf & vbCrLf, rfile)
                        End If
                        stage += 1
                    End If

                    If bInError = False Then
                        WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Delete old backup files" & vbCrLf, rfile)
                        DeleteOldFiles(fname, pOptype, foundRows(i)(0).ToString)
                        stage += 1
                    End If
                End If
            Next i
        Else
            If pOptype = "LOG" Then
                'Check for a log backup on a simple database
                foundRows = dt.Select("Name = '" & pDatabase & "' And Recovery <> 3")
                If foundRows.Length = 0 Then
                    bLogAttemptOnSimple = True
                    ShowMessage(vbCrLf & "Log backups not allowed for database " & pDatabase & " because its recovery mode is SIMPLE")
                    Exit Sub
                End If
            End If

            foundRows = dt.Select("Name = '" & pDatabase & "' and Status = 1")
            If foundRows.Length <> 0 And ((CInt(foundRows(0)(1)) And DatabaseStatus.Normal) = DatabaseStatus.Normal) Then
                start = Now()
                If bReport Then
                    WriteToFile("[" & CStr(stage) & "] Database " & pDatabase & ": Backup starting at " & Now.ToString & vbCrLf, rfile)
                End If
                Dim bk As Backup = New Backup
                SetBackupOptions(bk)
                bk.Database = pDatabase
                fname = GenerateFilename(pBackupFolder, pDatabase, pOptype, bCustomFilename)
                If fname = "-1" Then
                    Exit Sub
                Else
                    Dim bki As New BackupDeviceItem(fname, DeviceType.File)
                    bk.Devices.Add(bki)
                    filelist.Add(fname)
                End If
                bk.Initialize = True
                bk.SqlBackup(srv)
                bk = Nothing
                stage += 1
                If bReport Then
                    finish = Now()
                    If pOptype = "LOG" Then
                        WriteToFile("    Log backed up to " & fname & vbCrLf, rfile)
                    Else
                        WriteToFile("    Database backed up to " & fname & vbCrLf, rfile)
                    End If
                    WriteToFile("    Backup completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                End If

                If bVerify Then
                    start = Now()
                    If bReport Then
                        WriteToFile("[" & CStr(stage) & "] Database " & pDatabase & ": Verify starting at " & Now.ToString & vbCrLf, rfile)
                    End If
                    Dim verifyerror As String = ""
                    Dim bIsValid As Boolean = False
                    Dim res As Restore = New Restore
                    Dim rsi As New BackupDeviceItem(fname, DeviceType.File)
                    res.Devices.Add(rsi)
                    SetVerifyOptions(res)
                    bIsValid = res.SqlVerify(srv, verifyerror)
                    If bIsValid Then
                        If bReport Then
                            finish = Now()
                            WriteToFile("    Backup file " & fname & " verified" & vbCrLf, rfile)
                            WriteToFile("    Verify backup completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                        End If
                    Else
                        WriteToFile("    Backup file " & fname & " failed verification" & vbCrLf, rfile)
                        WriteToFile("    " & verifyerror & vbCrLf & vbCrLf, rfile)
                    End If
                    stage += 1
                End If
                If bReport Then
                    WriteToFile("[" & CStr(stage) & "] Database " & pDatabase & ": Delete old backup files" & vbCrLf, rfile)
                End If
                DeleteOldFiles(fname, pOptype, pDatabase)
                stage += 1
            Else
                Dim pmsg As String = "Database " & pDatabase & " does not exist or is not online" & vbCrLf
                Throw New SmoException(pmsg)
            End If
        End If
    End Sub

    'Do the Integrity Checks
    Private Sub DoCheckDB()
        Dim foundRows() As DataRow
        Dim start As DateTime
        Dim finish As DateTime
        Dim dbp As String = pDatabase.ToUpper
        Dim db As Database
        Dim sc As Specialized.StringCollection

        ' If it is a batch operation...
        If dbp = ALL_SYSTEM Or dbp = ALL_USER Or dbp = ALL Then

            Select Case dbp
                Case ALL_SYSTEM
                    foundRows = dt.Select("IsSystem = True And Status = 1", "Name ASC")
                Case ALL_USER
                    foundRows = dt.Select("IsSystem = False And Status = 1", "Name ASC")
                Case Else
                    foundRows = dt.Select("Status = 1", "Name ASC")
            End Select

            For i As Integer = 0 To foundRows.GetUpperBound(0)
                start = Now()
                If bReport Then
                    WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Starting CheckDB..." & vbCrLf & vbCrLf, rfile)
                End If

                Try
                    db = srv.Databases(foundRows(i)(0).ToString)
                    sc = db.CheckTables(RepairType.None)

                    If Not sc Is Nothing Then
                        For c As Integer = 0 To sc.Count - 1
                            If sc(c).Length > 0 Then
                                If bReport Then
                                    WriteToFile(sc(c), rfile)
                                Else
                                    Console.WriteLine(sc(c))
                                End If
                            End If
                        Next
                    End If

                    If bReport Then
                        finish = Now()
                        WriteToFile(vbCrLf & "    CheckDB completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                    End If

                Catch ex As SmoException
                    If Not bContinueOnError Then
                        Throw ex
                    Else
                        ShowError(ex)
                    End If
                Catch ex As Exception
                    If Not bContinueOnError Then
                        Throw ex
                    Else
                        ShowError(ex)
                    End If
                Finally
                    db = Nothing
                End Try
                stage += 1
            Next i

        Else ' Single Database operation
            start = Now()
            If bReport Then
                WriteToFile("[" & CStr(stage) & "] Database " & pDatabase & ": Starting CheckDB..." & vbCrLf & vbCrLf, rfile)
            End If

            db = srv.Databases(pDatabase)
            sc = db.CheckTables(RepairType.None)
            For c As Integer = 0 To sc.Count - 1
                If sc(c).Length > 0 Then
                    If bReport Then
                        WriteToFile(sc(c), rfile)
                    Else
                        Console.WriteLine(sc(c))
                    End If
                End If
            Next
            db = Nothing
            stage += 1
            If bReport Then
                finish = Now()
                WriteToFile(vbCrLf & "    CheckDB completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
            End If
        End If

    End Sub

    'Do the Reindex/Reorg
    Private Sub DoReindex()
        Dim foundRows() As DataRow
        Dim start As DateTime
        Dim finish As DateTime
        Dim dbp As String = pDatabase.ToUpper

        ' If it is a batch operation...
        If dbp = ALL_SYSTEM Or dbp = ALL_USER Or dbp = ALL Then
            Select Case dbp
                Case ALL_SYSTEM
                    foundRows = dt.Select("IsSystem = True And Status = 1", "Name ASC")
                Case ALL_USER
                    foundRows = dt.Select("IsSystem = False And Status = 1", "Name ASC")
                Case Else
                    foundRows = dt.Select("Status = 1", "Name ASC")
            End Select

            For i As Integer = 0 To foundRows.GetUpperBound(0)
                start = Now()
                If bReport Then
                    Select Case pOptype
                        Case "REINDEX"
                            WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Index Rebuild (using original fillfactor)..." & vbCrLf & vbCrLf, rfile)
                        Case "REORG"
                            WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Index Reorganize..." & vbCrLf & vbCrLf, rfile)
                    End Select
                End If
                Dim db As Database

                Try
                    db = srv.Databases(foundRows(i)(0).ToString)
                    If pOptype = "REINDEX" Then
                        For Each t As Table In db.Tables
                            If t.HasIndex And HasDisabledClusteredIndex(t) = False Then
                                If bReport Then
                                    WriteToFile("    Rebuilding indexes for table [" & t.Schema & "].[" & t.Name & "]" & vbCrLf, rfile)
                                End If
                                For Each ind As Index In t.Indexes
                                    If Not ind.IsDisabled Then
                                        ind.Rebuild()
                                    End If
                                Next
                            End If
                        Next
                    Else
                        ReorganizeIndexes(db.Name)
                    End If


                    If bReport Then
                        finish = Now()
                        WriteToFile(vbCrLf & "    Index maintenance completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                    End If
                Catch ex As SmoException
                    If Not bContinueOnError Then
                        Throw ex
                    Else
                        ShowError(ex)
                    End If
                Catch ex As Exception
                    If Not bContinueOnError Then
                        Throw ex
                    Else
                        ShowError(ex)
                    End If
                Finally
                    db = Nothing
                End Try

                stage += 1
            Next i

        Else ' Single Database operation
            Dim db As Database
            db = srv.Databases(pDatabase)
            start = Now()
            If bReport Then
                Select Case pOptype
                    Case "REINDEX"
                        WriteToFile("[" & CStr(stage) & "] Database " & pDatabase & ": Index Rebuild (using original fillfactor)..." & vbCrLf & vbCrLf, rfile)
                    Case "REORG"
                        WriteToFile("[" & CStr(stage) & "] Database " & pDatabase & ": Index Reorganize..." & vbCrLf & vbCrLf, rfile)
                End Select
            End If

            If pOptype = "REINDEX" Then
                For Each t As Table In db.Tables
                    If t.HasIndex And HasDisabledClusteredIndex(t) = False Then
                        If bReport Then
                            WriteToFile("    Rebuilding indexes for table [" & t.Schema & "].[" & t.Name & "]" & vbCrLf, rfile)
                        End If
                        For Each ind As Index In t.Indexes
                            If Not ind.IsDisabled Then
                                ind.Rebuild()
                            End If
                        Next
                    End If
                Next
            Else
                ReorganizeIndexes(db.Name)
            End If

            db = Nothing
            stage += 1
            If bReport Then
                finish = Now()
                WriteToFile(vbCrLf & "    Index maintenance completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
            End If
        End If

    End Sub

    'Do the statistics update
    Private Sub DoUpdateStats()
        Dim foundRows() As DataRow
        Dim start As DateTime
        Dim finish As DateTime
        Dim dbp As String = pDatabase.ToUpper

        ' If it is a batch operation...
        If dbp = ALL_SYSTEM Or dbp = ALL_USER Or dbp = ALL Then
            Select Case dbp
                Case ALL_SYSTEM
                    foundRows = dt.Select("IsSystem = True And Status = 1", "Name ASC")
                Case ALL_USER
                    foundRows = dt.Select("IsSystem = False And Status = 1", "Name ASC")
                Case Else
                    foundRows = dt.Select("Status = 1", "Name ASC")
            End Select

            For i As Integer = 0 To foundRows.GetUpperBound(0)
                start = Now()
                If bReport Then
                    Select Case pOptype
                        Case "STATS"
                            WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Update Statistics..." & vbCrLf & vbCrLf, rfile)
                        Case "STATSFULL"
                            WriteToFile("[" & CStr(stage) & "] Database " & foundRows(i)(0).ToString & ": Update Statistics with fullscan..." & vbCrLf & vbCrLf, rfile)
                    End Select
                End If
                Dim db As Database

                Try
                    db = srv.Databases(foundRows(i)(0).ToString)
                    If pOptype = "STATS" Then
                        db.UpdateIndexStatistics()
                    Else
                        For Each t As Table In db.Tables
                            If t.HasIndex And HasDisabledClusteredIndex(t) = False Then
                                If bReport Then
                                    WriteToFile("    Updating Statistics for table [" & t.Schema & "].[" & t.Name & "]" & vbCrLf, rfile)
                                End If
                                t.UpdateStatistics(StatisticsTarget.All, StatisticsScanType.FullScan)
                            End If
                        Next
                    End If

                    If bReport Then
                        finish = Now()
                        WriteToFile(vbCrLf & "    Statistics maintenance completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
                    End If
                Catch ex As SmoException
                    If Not bContinueOnError Then
                        Throw ex
                    Else
                        ShowError(ex)
                    End If
                Catch ex As Exception
                    If Not bContinueOnError Then
                        Throw ex
                    Else
                        ShowError(ex)
                    End If
                Finally
                    db = Nothing
                End Try

                stage += 1
            Next i

        Else ' Single Database operation
            Dim db As Database
            db = srv.Databases(pDatabase)
            start = Now()
            If bReport Then
                Select Case pOptype
                    Case "STATS"
                        WriteToFile("[" & CStr(stage) & "] Database " & pDatabase & ": Update Statistics..." & vbCrLf & vbCrLf, rfile)
                    Case "STATSFULL"
                        WriteToFile("[" & CStr(stage) & "] Database " & pDatabase & ": Update Statistics with fullscan..." & vbCrLf & vbCrLf, rfile)
                End Select
            End If

            If pOptype = "STATS" Then
                db.UpdateIndexStatistics()
            Else
                For Each t As Table In db.Tables
                    If t.HasIndex And HasDisabledClusteredIndex(t) = False Then
                        If bReport Then
                            WriteToFile("    Updating Statistics for table [" & t.Schema & "].[" & t.Name & "]" & vbCrLf, rfile)
                        End If
                        t.UpdateStatistics(StatisticsTarget.All, StatisticsScanType.FullScan)
                    End If
                Next
            End If

            db = Nothing
            stage += 1
            If bReport Then
                finish = Now()
                WriteToFile(vbCrLf & "    Statistics maintenance completed in " & finish.Subtract(start).Hours.ToString & " hour(s) " & finish.Subtract(start).Minutes.ToString & " min(s) " & finish.Subtract(start).Seconds.ToString & " second(s)" & vbCrLf & vbCrLf, rfile)
            End If
        End If

    End Sub

    'Delete Old Files
    Private Sub DeleteOldFiles(ByVal path As String, ByVal type As String, ByVal dbname As String)
        Dim basedir As String = Directory.GetParent(path).FullName
        Dim di As DirectoryInfo = New DirectoryInfo(basedir)
        Dim fi As FileInfo() = di.GetFiles()
        Dim db As String = dbname.Replace(" ", "_").Replace("'", "_").ToUpper()
        delcount = 0
        Try
            If type.ToUpper = "DB" Or type.ToUpper = "DIF" Then
                For Each fiTemp As FileInfo In fi
                    If bCustomFilename Then
                        If fiTemp.Name.ToUpper.IndexOf(db.ToUpper) >= 0 And fiTemp.Extension.ToUpper = ".BAK" And DoDelete(fiTemp, type.ToUpper, bCheckarchive) Then
                            fiTemp.Delete()
                            delcount += 1
                            If bReport Then
                                WriteToFile("    Deleted file " & basedir & "\" & fiTemp.Name & vbCrLf, rfile)
                            End If
                        End If
                    Else
                        If fiTemp.Name.ToUpper.IndexOf(db.ToUpper) >= 0 And fiTemp.Extension.ToUpper = ".BAK" And _
                        fiTemp.Name.IndexOf(FileFromOptype(pOptype)) >= 0 And DoDelete(fiTemp, type.ToUpper, bCheckarchive) Then
                            fiTemp.Delete()
                            delcount += 1
                            If bReport Then
                                WriteToFile("    Deleted file " & basedir & "\" & fiTemp.Name & vbCrLf, rfile)
                            End If
                        End If
                    End If
                Next fiTemp
            ElseIf type.ToUpper = "LOG" Then
                For Each fiTemp As FileInfo In fi
                    If bCustomFilename Then
                        If fiTemp.Name.ToUpper.IndexOf(db.ToUpper) >= 0 And fiTemp.Extension.ToUpper = ".TRN" And DoDelete(fiTemp, type.ToUpper, bCheckarchive) Then
                            fiTemp.Delete()
                            delcount += 1
                            If bReport Then
                                WriteToFile("    Deleted file " & basedir & "\" & fiTemp.Name & vbCrLf, rfile)
                            End If
                        End If
                    Else
                        If fiTemp.Name.ToUpper.IndexOf(db.ToUpper) >= 0 And fiTemp.Extension.ToUpper = ".TRN" And _
                         fiTemp.Name.IndexOf(FileFromOptype(pOptype)) >= 0 And DoDelete(fiTemp, type.ToUpper, bCheckarchive) Then
                            fiTemp.Delete()
                            delcount += 1
                            If bReport Then
                                WriteToFile("    Deleted file " & basedir & "\" & fiTemp.Name & vbCrLf, rfile)
                            End If
                        End If
                    End If
                Next fiTemp
            Else
                For Each fiTemp As FileInfo In fi
                    If fiTemp.Name.ToUpper.IndexOf(db.ToUpper) >= 0 And fiTemp.Extension.ToUpper = ".TXT" And _
                    fiTemp.Name.IndexOf(FileFromOptype(pOptype)) >= 0 And DoDelete(fiTemp, "REPORT", False) And fiTemp.Name <> rfile Then
                        fiTemp.Delete()
                        delcount += 1
                        If bReport Then
                            WriteToFile("    Deleted file " & basedir & "\" & fiTemp.Name & vbCrLf, rfile)
                        End If
                    End If
                Next fiTemp
            End If
        Catch e As IOException
            ShowError(e)
            Exit Sub
        Catch e As Exception
            ShowError(e)
            Exit Sub
        End Try
        If bReport Then
            If delcount = 0 Then
                WriteToFile("    0 file(s) deleted." & vbCrLf & vbCrLf, rfile)
            Else
                WriteToFile("    " & CStr(delcount) & " file(s) deleted." & vbCrLf & vbCrLf, rfile)
            End If

        End If

    End Sub

    ' Check date on files for deletion
    Private Function DoDelete(ByVal f As FileInfo, ByVal type As String, ByVal bCheckArchive As Boolean) As Boolean
        Dim bDoDelete As Boolean = False

        If debugargs Then
            ShowMessage("" + vbCrLf)
            ShowMessage("*************************************************" + vbCrLf)
            ShowMessage("Entered DoDelete" + vbCrLf)
            ShowMessage("*************************************************" + vbCrLf)
            ShowMessage("" + vbCrLf)
            ShowMessage("file             : " + f.FullName + vbCrLf)
            ShowMessage("type             : " + type + vbCrLf)
            ShowMessage("bCheckArchive    : " + bCheckArchive.ToString() + vbCrLf)
            ShowMessage("pDBRetainunit    : " + pDBRetainunit + vbCrLf)
            ShowMessage("pDBRetainvalue   : " + pDBRetainvalue.ToString() + vbCrLf)
            ShowMessage("pRPTRetainunit   : " + pRPTRetainunit + vbCrLf)
            ShowMessage("pRPTRetainvalue  : " + pRPTRetainvalue.ToString() + vbCrLf)
            ShowMessage("globalstart      : " + globalstart.ToString("yyyyMMdd HH:mm:ss") + vbTab + "(" + globalstart.ToString() + ")" + vbCrLf)
            ShowMessage("f.CreationTime   : " + f.CreationTime.ToString("yyyyMMdd HH:mm:ss") + vbTab + "(" + f.CreationTime.ToString() + ")" + vbCrLf)
            ShowMessage("f.LastAccessTime : " + f.LastAccessTime.ToString("yyyyMMdd HH:mm:ss") + vbTab + "(" + f.LastAccessTime.ToString() + ")" + vbCrLf)
            ShowMessage("f.LastWriteTime  : " + f.LastWriteTime.ToString("yyyyMMdd HH:mm:ss") + vbTab + "(" + f.LastWriteTime.ToString() + ")" + vbCrLf)

            If type <> "REPORT" Then
                ShowMessage("archivebitset  : " + ((f.Attributes And FileAttributes.Archive) <> FileAttributes.Archive).ToString() + vbCrLf)
            End If
        End If

        'ignore files we created in this execution
        If filelist.Contains(f.FullName) Then
            If debugargs Then
                ShowMessage("filelist.Contains(f.FullName) = True" + vbCrLf)
                ShowMessage("bDoDelete      : False" + vbCrLf)
                ShowMessage("" + vbCrLf)
            End If

            Return False
        End If

        If type = "REPORT" Then
            Select Case pRPTRetainunit
                Case "MINUTES"
                    If f.CreationTime.AddMinutes(CDbl(pRPTRetainvalue)) < globalstart Then
                        bDoDelete = True
                    Else
                        bDoDelete = False
                    End If
                Case "HOURS"
                    If f.CreationTime.AddHours(CDbl(pRPTRetainvalue)) < globalstart Then
                        bDoDelete = True
                    Else
                        bDoDelete = False
                    End If
                Case "DAYS"
                    If f.CreationTime.AddDays(CDbl(pRPTRetainvalue)) < globalstart Then
                        bDoDelete = True
                    Else
                        bDoDelete = False
                    End If
                Case "WEEKS"
                    If f.CreationTime.AddDays(CDbl(pRPTRetainvalue * 7)) < globalstart Then
                        bDoDelete = True
                    Else
                        bDoDelete = False
                    End If
            End Select
        Else
            Select Case pDBRetainunit
                Case "MINUTES"
                    If f.CreationTime.AddMinutes(CDbl(pDBRetainvalue)) < globalstart _
                    And (bCheckArchive = False Or ((f.Attributes And FileAttributes.Archive) <> FileAttributes.Archive)) Then
                        bDoDelete = True
                    Else
                        bDoDelete = False
                    End If
                Case "HOURS"
                    If f.CreationTime.AddHours(CDbl(pDBRetainvalue)) < globalstart _
                    And (bCheckArchive = False Or ((f.Attributes And FileAttributes.Archive) <> FileAttributes.Archive)) Then
                        bDoDelete = True
                    Else
                        bDoDelete = False
                    End If
                Case "DAYS"
                    If f.CreationTime.AddDays(CDbl(pDBRetainvalue)) < globalstart _
                    And (bCheckArchive = False Or ((f.Attributes And FileAttributes.Archive) <> FileAttributes.Archive)) Then
                        bDoDelete = True
                    Else
                        bDoDelete = False
                    End If
                Case "WEEKS"
                    If f.CreationTime.AddDays(CDbl(pDBRetainvalue * 7)) < globalstart _
                    And (bCheckArchive = False Or ((f.Attributes And FileAttributes.Archive) <> FileAttributes.Archive)) Then
                        bDoDelete = True
                    Else
                        bDoDelete = False
                    End If
            End Select
        End If

        If debugargs Then
            ShowMessage("bDoDelete      : " + bDoDelete.ToString + vbCrLf)
            ShowMessage("" + vbCrLf)
        End If

        Return bDoDelete
    End Function

    'Parse command line arguments
    Private Function ParseArguments(ByRef args As String()) As Boolean
        If args(1) = "-?" Or args(1) = "?" Or args(1) = "/?" Then
            ShowMessage(Usage)
            Return False
        Else
            Try
                Dim i As Integer = 0
                Dim s As String
                For i = 1 To UBound(args)
                    s = args(i)
                    If s.ToUpper = "-S" Then
                        pServer = args(i + 1)
                    End If
                    If s.ToUpper = "-D" Then
                        pDatabase = args(i + 1)
                    End If
                    If s.ToUpper = "-U" Then
                        sqllogin = args(i + 1)
                        bIsTrustedConn = False
                    End If
                    If s.ToUpper = "-P" Then
                        sqlpass = args(i + 1)
                    End If
                    If s.ToUpper = "-T" Then
                        pOptype = args(i + 1).ToUpper
                    End If
                    If s.ToUpper = "-B" Then
                        pBackupFolder = args(i + 1)
                    End If
                    If s.ToUpper = "-R" Then
                        pReportFolder = args(i + 1)
                        bReport = True
                    End If
                    If s.ToUpper = "-V" Then
                        bVerify = True
                    End If
                    If s.ToUpper = "-BU" Then
                        pDBRetainunit = args(i + 1).ToUpper
                    End If
                    If s.ToUpper = "-BV" Then
                        pDBRetainvalue = CInt(args(i + 1))
                    End If
                    If s.ToUpper = "-RU" Then
                        pRPTRetainunit = args(i + 1).ToUpper
                    End If
                    If s.ToUpper = "-RV" Then
                        pRPTRetainvalue = CInt(args(i + 1))
                    End If
                    If s.ToUpper = "-A" Then
                        bCheckarchive = True
                    End If
                    If s.ToUpper = "-DS" Then
                        bTimeStampFirst = True
                    End If
                    If s.ToUpper = "-TO" Then
                        querytimeout = CInt(args(i + 1))
                    End If
                    If s.ToUpper = "-C" Then
                        bContinueOnError = True
                    End If
                    If s.ToUpper = "-BF" Then
                        customfilename = args(i + 1)
                        bCustomFilename = True
                    End If
                    ' If debug print parameters
                    ' -X has to be first parameter
                    If s.ToUpper = "-X" Then
                        debugargs = True
                    End If
                    If debugargs Then
                        Console.WriteLine(CStr(i) & ": " & s)
                    End If
                Next i
            Catch ex As Exception
                ShowMessage("Error parsing parameters")
                Return False
            End Try

            'Now validate parameters
            If pServer Is Nothing Or pDatabase Is Nothing Or pOptype Is Nothing Then
                ShowMessage(vbCrLf & "The values for the -S -D -T parameters are required and cannot be ommitted")
                Return False
            End If

            If (Not String.IsNullOrEmpty(sqllogin)) And (String.IsNullOrEmpty(sqlpass)) Then
                ShowMessage(vbCrLf & "The value for the -P parameter is required if the -U parameter is supplied and cannot be ommitted")
                Return False
            End If

            If pOptype <> "DB" And pOptype <> "DIF" And pOptype <> "LOG" And pOptype <> "REINDEX" And pOptype <> "REORG" And pOptype <> "CHECKDB" And pOptype <> "STATS" And pOptype <> "STATSFULL" Then
                ShowMessage(vbCrLf & pOptype & " is an invalid value for switch -T")
                Return False
            End If

            If pOptype = "DB" Or pOptype = "DIF" Or pOptype = "LOG" Then
                If pDBRetainunit Is Nothing Then
                    ShowMessage(vbCrLf & "A value for switch -BU must be specified for a backup operation")
                    Return False
                End If
                If pDBRetainunit <> "HOURS" And pDBRetainunit <> "DAYS" And pDBRetainunit <> "WEEKS" And pDBRetainunit <> "MINUTES" Then
                    ShowMessage(vbCrLf & pDBRetainunit & " is an invalid value for switch -BU")
                    Return False
                End If
                If Not IsNumeric(pDBRetainvalue) Or pDBRetainvalue < 1 Then
                    ShowMessage(vbCrLf & pDBRetainvalue & " is an invalid value for switch -BV")
                    Return False
                End If
                If Not Directory.Exists(pBackupFolder) Then
                    ShowMessage(vbCrLf & "Backup folder " & pBackupFolder & " not found")
                    Return False
                End If
                If bCustomFilename Then
                    If customfilename.IndexOf("$(DB)") < 0 Then
                        ShowMessage(vbCrLf & "Custom backup format (" & customfilename & ") is not valid. Must contain $(DB) token")
                        Return False
                    End If
                    If customfilename.IndexOf("$(DATE)") < 0 Then
                        ShowMessage(vbCrLf & "Custom backup format (" & customfilename & ") is not valid. Must contain $(DATE) token")
                        Return False
                    End If
                End If
            End If

            If bReport Then
                If pRPTRetainunit Is Nothing Or pRPTRetainunit = "" Then
                    ShowMessage(vbCrLf & "A value for switch -RU must be specified when -R is present")
                    Return False
                End If
                If pRPTRetainunit <> "HOURS" And pRPTRetainunit <> "DAYS" And pRPTRetainunit <> "WEEKS" And pRPTRetainunit <> "MINUTES" Then
                    ShowMessage(vbCrLf & pRPTRetainunit & " is an invalid value for switch -RU")
                    Return False
                End If
                If Not IsNumeric(pRPTRetainvalue) Or pRPTRetainvalue < 1 Then
                    ShowMessage(vbCrLf & pRPTRetainvalue & " is an invalid value for switch -RV")
                    Return False
                End If
                If Not Directory.Exists(pReportFolder) Then
                    ShowMessage(vbCrLf & "Report folder " & pReportFolder & " not found")
                    Return False
                End If
            End If

            'All ok
            Return True
        End If
    End Function

    'Derive filename part from Optype
    Private Function FileFromOptype(ByRef pOptype As String) As String
        Select Case pOptype
            Case "DB"
                FileFromOptype = "FullBackup"
                Exit Select
            Case "DIF"
                FileFromOptype = "DiffBackup"
                Exit Select
            Case "LOG"
                FileFromOptype = "LogBackup"
                Exit Select
            Case "CHECKDB"
                FileFromOptype = "CheckDB"
                Exit Select
            Case "REINDEX"
                FileFromOptype = "Reindex"
                Exit Select
            Case "REORG"
                FileFromOptype = "Reorg"
                Exit Select
            Case "STATS", "STATSFULL"
                FileFromOptype = "Stats"
                Exit Select
        End Select
    End Function

    'Generate backup filenames
    Private Function GenerateFilename(ByVal path As String, ByVal db As String, ByVal type As String, ByVal custom As Boolean) As String

        Dim d As Date = Now()
        Dim f As String = String.Empty
        If Right(path, 1) <> "\" Then
            path += "\"
        End If
        path += db.Replace(" ", "_").Replace("'", "_") + "\"
        If Not Directory.Exists(path) Then
            Try
                Directory.CreateDirectory(path)
            Catch ex As Exception
                ShowError(ex)
                GenerateFilename = "-1"
                Exit Function
            End Try
        End If

        If custom = False Then

            If bTimeStampFirst Then
                f = path & d.Year.ToString & Right("0" + d.Month.ToString, 2) & Right("0" + d.Day.ToString, 2) & "_" & d.ToString("HHmm") & "_" & _
                db.Replace(" ", "_").Replace("'", "_") & "_" & FileFromOptype(pOptype)
            Else
                f = path & db.Replace(" ", "_").Replace("'", "_") & "_" & FileFromOptype(pOptype) & "_" & _
                d.Year.ToString & Right("0" + d.Month.ToString, 2) & Right("0" + d.Day.ToString, 2) & "_" & d.ToString("HHmm")
            End If

            Select Case type
                Case "DB", "DIF"
                    f += ".bak"
                Case "LOG"
                    f += ".trn"
            End Select
            GenerateFilename = f
        Else
            'replace tokens in custom filename format
            '$(DB)     = Database Name
            '$(DATE)   = Date (YYYYMMDD)
            '$(TIME)   = Time (HHmm)
            '$(OPTYPE) = Operation Type
            f = path & customfilename.Replace("$(DB)", db.Replace(" ", "_").Replace("'", "_")).Replace("$(DATE)", d.Year.ToString & Right("0" + d.Month.ToString, 2) & Right("0" + d.Day.ToString, 2)).Replace("$(TIME)", d.ToString("HHmm")).Replace("$(OPTYPE)", FileFromOptype(pOptype))
            Select Case type
                Case "DB", "DIF"
                    f += ".bak"
                Case "LOG"
                    f += ".trn"
            End Select
            GenerateFilename = f
        End If
    End Function

    'Generate report filename
    Private Function GenerateReportFilename(ByVal path As String, ByVal db As String) As Boolean
        Dim d As Date = Now()
        Dim f As String
        If Right(path, 1) <> "\" Then
            path += "\"
        End If
        If bTimeStampFirst Then
            f = path & d.Year.ToString & Right("0" + d.Month.ToString, 2) & Right("0" + d.Day.ToString, 2) & "_" & d.ToString("HHmm") & "_" & _
            db.ToUpper.Replace(" ", "_").Replace("'", "_") & "_" & FileFromOptype(pOptype) & ".txt"
        Else
            f = path & db.ToUpper.Replace(" ", "_").Replace("'", "_") & "_" & FileFromOptype(pOptype) & "_" & _
            d.Year.ToString & Right("0" + d.Month.ToString, 2) & Right("0" + d.Day.ToString, 2) & "_" & d.ToString("HHmm") & ".txt"
        End If
        rfile = f
        Return True
    End Function

    ' Set any additional backup options
    Private Sub SetBackupOptions(ByRef bk As Backup)
        'If Not (pBackupwith Is Nothing) Then
        '    If pBackupwith.IndexOf("CHECKSUM") > 0 Then
        '        bk.Checksum = True
        '    Else
        '        bk.Checksum = False
        '    End If
        '    If pBackupwith.IndexOf("CONTINUE_AFTER_ERROR") > 0 Then
        '        bk.ContinueAfterError = True
        '    Else
        '        bk.Checksum = False
        '    End If
        'End If

        Select Case pOptype
            Case "DB"
                bk.Action = BackupActionType.Database
                bk.Incremental = False
            Case "DIF"
                bk.Action = BackupActionType.Database
                bk.Incremental = True
            Case "LOG"
                bk.Action = BackupActionType.Log
        End Select
    End Sub

    ' Set any additional verify options
    Private Sub SetVerifyOptions(ByRef res As Restore)
        Select Case pOptype
            Case "DB", "DIFF"
                res.Action = RestoreActionType.Database
            Case "LOG"
                res.Action = RestoreActionType.Log
        End Select
    End Sub

    'Write to log file
    Private Sub WriteToFile(ByVal content As String, ByVal path As String)
        If Not bReport Then
            Exit Sub
        End If

        Dim sw As StreamWriter

        Try
            sw = New StreamWriter(path, True)
            sw.Write(content)
        Catch ex As IOException
            Console.WriteLine(ex.Message)
            Console.Write(ex.StackTrace)
            If Not ex.InnerException Is Nothing Then
                Console.WriteLine("")
                Console.WriteLine(ex.InnerException.Message)
                Console.Write(ex.StackTrace)
            End If
        Catch ex As Exception
            Console.WriteLine(ex.Message)
            Console.Write(ex.StackTrace)
            If Not ex.InnerException Is Nothing Then
                Console.WriteLine("")
                Console.WriteLine(ex.InnerException.Message)
                Console.Write(ex.StackTrace)
            End If
        Finally
            If Not sw Is Nothing Then
                sw.Close()
            End If
        End Try
    End Sub

    'Generic Error Display
    Private Sub ShowError(ByVal ex As Exception)
        If bReport And File.Exists(rfile) Then
            WriteToFile(vbTab + ex.Message & vbCrLf, rfile)
            If ex.InnerException Is Nothing Then
                Exit Sub
            Else
                ShowError(ex.InnerException)
            End If
        Else
            Console.WriteLine("Expressmaint error : " + ex.Message)
            If ex.InnerException Is Nothing Then
                Exit Sub
            Else
                ShowError(ex.InnerException)
            End If
        End If
    End Sub

    'Generic Message Display
    Private Sub ShowMessage(ByVal msg As String)
        If bReport And File.Exists(rfile) Then
            WriteToFile(msg, rfile)
        Else
            Console.WriteLine(msg)
        End If
    End Sub

    'App level Error Handler
    Sub MyHandler(ByVal sender As Object, ByVal args As UnhandledExceptionEventArgs)
        Dim e As Exception = DirectCast(args.ExceptionObject, Exception)
        ShowError(e)
    End Sub

    'Utility method to workaround SMO Reorganize bug (https://connect.microsoft.com/SQLServer/feedback/ViewFeedback.aspx?FeedbackID=339570)
    Private Sub ReorganizeIndexes(ByVal db As String)

        Dim ds As DataSet = New DataSet()
        Dim c As SqlConnection = New SqlConnection(conn.ConnectionString)
        c.Open()

        Dim reorgcmd As SqlCommand = New SqlCommand()
        reorgcmd.Connection = c
        reorgcmd.CommandType = CommandType.Text
        reorgcmd.CommandTimeout = (querytimeout * 60)

        Dim cmd As SqlCommand = New SqlCommand()
        cmd.Connection = c
        cmd.CommandType = CommandType.Text
        cmd.CommandText = "select quotename(s.name) + '.' + quotename(t.name) as TableName,quotename(i.name) as IndexName " & _
                          "from [" + db + "].sys.indexes i " & _
                          "join [" + db + "].sys.tables t on i.object_id = t.object_id " & _
                          "join [" + db + "].sys.schemas s on t.schema_id = s.schema_id " & _
                          "where i.type in(1,2) and i.allow_page_locks = 1 and i.is_disabled = 0 " & _
                          "and i.object_id not in (select distinct object_id from [" + db + "].sys.indexes where type = 1 and is_disabled = 1) " & _
                          "order by TableName,IndexName"

        Dim adp As SqlDataAdapter = New SqlDataAdapter(cmd)
        adp.Fill(ds)
        cmd.Dispose()

        For Each r As DataRow In ds.Tables(0).Rows
            Dim table As String = r(0).ToString()
            Dim index As String = r(1).ToString()

            If bReport Then
                WriteToFile("    Reorganizing index " & table & "." & index & vbCrLf, rfile)
            End If

            reorgcmd.CommandText = "USE [" + db + "] ALTER INDEX " & index & " ON " & table & " REORGANIZE;"
            reorgcmd.ExecuteNonQuery()
        Next

        reorgcmd.Dispose()
        ds.Dispose()
        c.Close()
    End Sub


End Module