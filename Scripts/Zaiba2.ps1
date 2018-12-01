[CmdletBinding()]
Param(
    $influxdb_host = "localhost",
    $influxdb_port = "8086",
    $influxdb_db = "zaiba2",

    $mssql_datasource = "localhost",
    $mssql_userid = "",
    $mssql_password = "",
    $mssql_initialcatalog = "master",
    $mssql_application_name = "MSSQL Monitor Zaiba2",
    $mssql_application_intent = "ReadWrite",

    $sleep_interval = 5,

    [switch]$AzureSQLDB
)
$ErrorActionPreference = "Continue"

function Get-TimeStamp(){
    $base = New-Object -Type System.DateTime -ArgumentList 1970, 1, 1, 0,0,0,0
    return [bigint]($((Get-Date).ToUniversalTime()) - $base).TotalMilliSeconds * 1000000
}

function New-LineData($measurement, $tags, $fields, $timestamp){
    # Data Format :
    # https://docs.influxdata.com/influxdb/latest/write_protocols/line_protocol_reference/

    $line_tags = @()  
    $line_tags += $tags.Keys | %{
        "{0}={1}" -f $_, $tags[$_]
    }
    $line_fields = @()
    $line_fields += $fields.Keys | %{
        "{0}={1}" -f $_, $fields[$_]
    }

    $line_data = "{0},{1} {2} {3}" -f $measurement, ($line_tags -join ","), ($line_fields -join ",") , $timestamp
    return $line_data
}

function Write-LineData($baseuri, $data){
    $body = [System.Text.Encoding]::UTF8.GetBytes(($data -join "`n"))
    Invoke-RestMethod -Method Post -Uri $baseuri -Body $body > $null
}

############### Query definition #############################################################
$sql = @()
$sql += @"
SELECT 
	* 
FROM
(
	SELECT
		@@SERVERNAME AS server_name,
		COALESCE(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS sql_instance_name,
		DB_NAME() AS database_name,
		RTRIM(
			SUBSTRING(
			object_name, 
			PATINDEX('%:%', object_name) + 1,
			LEN(object_name) - PATINDEX('%:%', object_name) 
			)) AS object_name,
		RTRIM(counter_name) AS counter_name,
		CASE instance_name
			WHEN '' THEN ' '
			ELSE RTRIM(instance_name) 
		END AS instance_name,
		cntr_value,
		cntr_type
	FROM 
		sys.dm_os_performance_counters WITH(NOLOCK)
) AS T
WHERE 
	object_name IN('SQL Statistics', 'Buffer Manager', 'General Statistics', 'Locks', 'SQL Errors', 'Access Methods')
OPTION (RECOMPILE, MAXDOP 1);
"@

$sql += @"
SELECT
	*
FROM
(SELECT
    @@SERVERNAME AS server_name,
	COALESCE(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS sql_instance_name,
	DB_NAME(database_id) AS database_name,
	file_id,
	num_of_reads,
	num_of_bytes_read,
	io_stall_read_ms,
	num_of_writes,
	num_of_bytes_written,
	io_stall_write_ms,
	size_on_disk_bytes
FROM 
	sys.dm_io_virtual_file_stats(NULL, NULL)) AS T1 
WHERE
	database_name IS NOT NULL
ORDER BY
	database_name ASC, file_id ASC
OPTION (RECOMPILE, MAXDOP 1);
"@

$sql += @"
SELECT
	server_name,
	sql_instance_name,
	database_name,
	instance_name,
	CAST([CPU usage %] AS float) / CAST([CPU usage % base] AS float) AS [CPU Usage]
FROM
(
SELECT
    @@SERVERNAME AS server_name,
	COALESCE(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS sql_instance_name,
    DB_NAME() AS database_name,
	RTRIM(
		SUBSTRING(
		object_name, 
		PATINDEX('%:%', object_name) + 1,
		LEN(object_name) - PATINDEX('%:%', object_name) 
		)) AS object_name,
	RTRIM(counter_name) AS counter_name,
	CASE instance_name
		WHEN '' THEN ' '
		ELSE RTRIM(instance_name) 
	END AS instance_name,
	cntr_value
FROM 
	sys.dm_os_performance_counters WITH(NOLOCK)
WHERE 
	object_name LIKE '%Workload Group Stats%'
	AND
	counter_name IN('CPU usage %','CPU usage % base')
) AS T
PIVOT(
	SUM(cntr_value)
	FOR counter_name IN ([CPU usage %], [CPU usage % base])
) AS PV
OPTION(RECOMPILE, MAXDOP 1);
"@

$sql += @"
DECLARE @ProductVersion nvarchar(128) = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128))
DECLARE @MajorVersion int = (SELECT SUBSTRING(@ProductVersion, 1, CHARINDEX('.', @ProductVersion) - 1))
DECLARE @sql nvarchar(max) = '
SELECT 
	@@SERVERNAME AS server_name,
	COALESCE(SERVERPROPERTY(''InstanceName''), ''MSSQLSERVER'') AS sql_instance_name,
	DB_NAME() AS database_name,
	type, 
	name,
	SUM(%%pages%% + awe_allocated_kb) AS size_kb
FROM 
	sys.dm_os_memory_clerks WITH(NOLOCK)
GROUP BY 
	type,
	name
OPTION (RECOMPILE, MAXDOP 1);
'

IF @MajorVersion <= 10
BEGIN
	SET @sql = REPLACE(@sql, '%%pages%%' , 'single_pages_kb + multi_pages_kb')
END
ELSE
BEGIN
	SET @sql = REPLACE(@sql, '%%pages%%' , 'pages_kb')
END

EXECUTE(@sql);
"@

if(! $AzureSQLDB){
$sql += @"
SELECT
	@@SERVERNAME AS server_name,
	COALESCE(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS sql_instance_name,
	DB_NAME() AS database_name,
	SUM(current_tasks_count) AS current_tasks_count,
	SUM(runnable_tasks_count) AS runnable_tasks_count,
	SUM(current_workers_count) AS current_workers_count,
	SUM(active_workers_count) AS active_workers_count,
	SUM(work_queue_count) AS work_queue_count,
	(SELECT max_workers_count FROM sys.dm_os_sys_info) AS max_workers_count
FROM 
	sys.dm_os_schedulers WITH(NOLOCK)
WHERE 
	status = 'VISIBLE ONLINE'
OPTION (RECOMPILE, MAXDOP 1);
"@
}else{
$sql += @"
SELECT
	@@SERVERNAME AS server_name,
	COALESCE(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS sql_instance_name,
	DB_NAME() AS database_name,
	SUM(current_tasks_count) AS current_tasks_count,
	SUM(runnable_tasks_count) AS runnable_tasks_count,
	SUM(current_workers_count) AS current_workers_count,
	SUM(active_workers_count) AS active_workers_count,
	SUM(work_queue_count) AS work_queue_count,
	0 AS max_workers_count
FROM 
	sys.dm_os_schedulers WITH(NOLOCK)
WHERE 
	status = 'VISIBLE ONLINE'
OPTION (RECOMPILE, MAXDOP 1);
"@
}

$sql += @"
SELECT
	@@SERVERNAME AS server_name,
	COALESCE(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS sql_instance_name,
	DB_NAME() AS database_name, 
	wt.session_id,
	COALESCE(er.status, ' ') AS status,
	COALESCE(er.command, ' ') AS command,
	COALESCE(er.wait_type, ' ') AS wait_type,
	COALESCE(es.host_name, ' ') AS host_name,
	COALESCE(es.program_name, ' ') AS program_name,
	COALESCE(datediff(SECOND,  er.start_time, GETDATE()), 0) AS elapsed_time_sec,
	CASE at.transaction_type
		WHEN 2 THEN 0
		ELSE COALESCE(datediff(SECOND, at.transaction_begin_time, GETDATE()), 0)
	END AS transaction_elapsed_time_sec
FROM
	sys.dm_os_waiting_tasks AS wt WITH(NOLOCK)
	LEFT JOIN sys.dm_exec_requests AS er WITH(NOLOCK) ON wt.session_id = er.session_id
	LEFT JOIN sys.dm_tran_active_transactions AS at WITH(NOLOCK) ON at.transaction_id = er.transaction_id
	LEFT JOIN sys.dm_exec_sessions AS es WITH(NOLOCK) ON es.session_id = er.session_id
WHERE
	wt.session_id > 0
ORDER BY
	wt.session_id
OPTION (RECOMPILE, MAXDOP 1)
"@

$sql += @"
;WITH waitcategorystats ( 
	wait_category, wait_type, wait_time_ms, 
    waiting_tasks_count, 
    max_wait_time_ms) 
AS (
	SELECT 
		CASE 
			WHEN wait_type LIKE 'LCK%' THEN 'LOCKS' 
			WHEN wait_type LIKE 'PAGEIO%' THEN 'PAGE I/O LATCH' 
			WHEN wait_type LIKE 'PAGELATCH%' THEN 'PAGE LATCH (non-I/O)' 
			WHEN wait_type LIKE 'LATCH%' THEN 'LATCH (non-buffer)' 
			WHEN wait_type LIKE 'LATCH%' THEN 'LATCH (non-buffer)' 
			ELSE wait_type 
		END AS wait_category, 
		wait_type, 
		wait_time_ms, 
		waiting_tasks_count, 
		max_wait_time_ms 
	FROM   
		sys.dm_os_wait_stats WITH(NOLOCK)
    WHERE  
		wait_type NOT IN ( 
			'LAZYWRITER_SLEEP', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT' ,
			'REQUEST_FOR_DEADLOCK_SEARCH', 'BACKUPTHREAD', 'CHECKPOINT_QUEUE', 
			'EXECSYNC', 'FFT_RECOVERY', 
			'SNI_CRITICAL_SECTION', 'SOS_PHYS_PAGE_CACHE', 
			'CXROWSET_SYNC', 'DAC_INIT', 'DIRTY_PAGE_POLL', 
			'PWAIT_ALL_COMPONENTS_INITIALIZED', 'MSQL_XP', 'WAIT_FOR', 
			'DBMIRRORING_CMD', 'DBMIRROR_DBM_EVENT', 'DBMIRROR_EVENTS_QUEUE', 
			'DBMIRROR_WORKER_QUEUE', 'XE_TIMER_EVENT', 'XE_DISPATCHER_WAIT', 
			'WAITFOR_TASKSHUTDOWN', 'WAIT_FOR_RESULTS', 
			'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'WAITFOR' ,'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP' ,
			'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 
			'LOGMGR_QUEUE', 'FSAGENT' ) 
		AND wait_type NOT LIKE 'PREEMPTIVE%' 
		AND wait_type NOT LIKE 'SQLTRACE%' 
		AND wait_type NOT LIKE 'SLEEP%' 
		AND wait_type NOT LIKE 'FT_%' 
		AND wait_type NOT LIKE 'XE%' 
		AND wait_type NOT LIKE 'BROKER%' 
		AND wait_type NOT LIKE 'DISPATCHER%' 
		AND wait_type NOT LIKE 'PWAIT%' 
		AND wait_type NOT LIKE 'SP_SERVER%'
) 
SELECT 
        @@SERVERNAME AS server_name,
        COALESCE(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS sql_instance_name,
        DB_NAME() AS database_name, 
        wait_category, 
        Sum(wait_time_ms)        AS wait_time_ms, 
        Sum(waiting_tasks_count) AS waiting_tasks_count, 
        Max(max_wait_time_ms)    AS max_wait_time_ms 
FROM   waitcategorystats 
WHERE  wait_time_ms > 1000 
GROUP  BY wait_category  
OPTION (RECOMPILE, MAXDOP 1);
"@
######################################################################################

# https://docs.influxdata.com/influxdb/v1.7/tools/api/#write-http-endpoint
$baseuri = ("http://{0}:{1}/write?db={2}" -f $influxdb_host, $influxdb_port, $influxdb_db)

$constring = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
$constring.psbase.DataSource = $mssql_datasource
$constring.psbase.UserID = $mssql_userid
$constring.psbase.Password = $mssql_password
$constring.psbase.InitialCatalog = $mssql_initialcatalog
$constring.psbase.ApplicationName = $mssql_application_name
$constring.psbase.ApplicationIntent = $mssql_application_intent

$con = New-Object System.Data.SqlClient.SqlConnection
$con.ConnectionString = $constring

try{
    $con.Open()
}catch{
    "{0} : {1}" -f "Connection Open Error" , $Error[0].Exception
    Exit -1
}

$cmd = $con.CreateCommand()
$cmd.CommandType = [System.Data.CommandType]::Text
$cmd.CommandText = $sql

$da = New-Object System.Data.SqlClient.SqlDataAdapter
$da.SelectCommand = $cmd

while($true){
    $ds = New-Object System.Data.DataSet
    [void]$da.Fill($ds)
    
    $ds.Tables[0].TableName = "perf"
    $ds.Tables[1].TableName = "filestats"
    $ds.Tables[2].TableName = "cpustats"
    $ds.Tables[3].TableName = "memoryclerk"
    $ds.Tables[4].TableName = "workerthread"
    $ds.Tables[5].TableName = "waittask"
    $ds.Tables[6].TableName = "waitstats"

    $timestamp = Get-TimeStamp

    $data = @()
    foreach($row in $ds.Tables["perf"].Rows){
        $measurement = ($row.object_name -replace " ","\ ")
        $tags = @{
            "server_name"=($row.server_name -replace " ","\ ")
            "sql_instance_name" = ($row.sql_instance_name -replace " ","\ ")
            "database_name" = ($row.database_name -replace " ","\ ")
            "instance_name" = ($row.instance_name -replace " ","\ ")
            "counter_name" = ($row.counter_name -replace " ","\ ")
            "cntr_type" = $row.cntr_type
        }

        $fields = @{
            "cntr_value" = $row.cntr_value
        }
        $ret = New-LineData $measurement $tags $fields $timestamp
        $data += $ret
    }
    Write-LineData -baseuri $baseuri -data $data
    
    $data = @()
    foreach($row in $ds.Tables["filestats"].Rows){
        $measurement = "filestats"
        $tags = @{
            "server_name"=($row.server_name -replace " ","\ ")
            "sql_instance_name" = ($row.sql_instance_name -replace " ","\ ")
            "database_name" = ($row.database_name -replace " ","\ ")
            "file_id" = $row.file_id
        }

        $fields = @{
            "num_of_reads" = $row.num_of_reads
            "num_of_bytes_read" = $row.num_of_bytes_read
            "io_stall_read_ms" = $row.io_stall_read_ms
            "num_of_writes" = $row.num_of_writes
            "num_of_bytes_written" = $row.num_of_bytes_written 
            "io_stall_write_ms" = $row.io_stall_write_ms
            "size_on_disk_bytes" = $row.size_on_disk_bytes
        }
        $ret = New-LineData $measurement $tags $fields $timestamp
        $data += $ret
    }
    Write-LineData -baseuri $baseuri -data $data

    $data = @()
    foreach($row in $ds.Tables["cpustats"].Rows){
        $measurement = "cpustats"
        $tags = @{
            "server_name"=($row.server_name -replace " ","\ ")
            "sql_instance_name" = ($row.sql_instance_name -replace " ","\ ")
            "database_name" = ($row.database_name -replace " ","\ ")
            "instance_name" = ($row.instance_name -replace " ","\ ")
        }

        $fields = @{
            "CPU_Usage" = $row."CPU Usage"
        }
        $ret = New-LineData $measurement $tags $fields $timestamp
        $data += $ret
    }
    Write-LineData -baseuri $baseuri -data $data

    $data = @()
    foreach($row in $ds.Tables["memoryclerk"].Rows){
        $measurement = "memoryclerk"
        $tags = @{
            "server_name"=($row.server_name -replace " ","\ ")
            "sql_instance_name" = ($row.sql_instance_name -replace " ","\ ")
            "database_name" = ($row.database_name -replace " ","\ ")
            "type" = ($row.type -replace " ","\ ")
            "name" = ($row.name -replace " ","\ ")
        }

        $fields = @{
            "size_kb" = $row.size_kb
        }
        $ret = New-LineData $measurement $tags $fields $timestamp
        $data += $ret
    }
    Write-LineData -baseuri $baseuri -data $data

    $data = @()
    foreach($row in $ds.Tables["workerthread"].Rows){
        $measurement = "workerthread"
        $tags = @{
            "server_name"=($row.server_name -replace " ","\ ")
            "sql_instance_name" = ($row.sql_instance_name -replace " ","\ ")
            "database_name" = ($row.database_name -replace " ","\ ")
        }

        $fields = @{
            "current_tasks_count" = $row.current_tasks_count
            "runnable_tasks_count" = $row.runnable_tasks_count
            "current_workers_count" = $row.current_workers_count
            "active_workers_count" = $row.active_workers_count
            "work_queue_count" = $row.work_queue_count
            "max_workers_count" = $row.max_workers_count
        }
        $ret = New-LineData $measurement $tags $fields $timestamp
        $data += $ret
    }
    Write-LineData -baseuri $baseuri -data $data

    $data = @()
    foreach($row in $ds.Tables["waittask"].Rows){
        $measurement = "waittask"
        $tags = @{
            "server_name"=($row.server_name -replace " ","\ ")
            "sql_instance_name" = ($row.sql_instance_name -replace " ","\ ")
            "database_name" = ($row.database_name -replace " ","\ ")
            "session_id" = $row.session_id
            "status" = ($row.status -replace " ","\ ")
            "command" = ($row.command -replace " ","\ ")
            "wait_type" = ($row.wait_type -replace " ","\ ")
            "host_name" = ($row.host_name -replace " ","\ ")
            "program_name" = ($row.program_name -replace " ","\ ")
        }

        $fields = @{
            "session_id" = $row.session_id
            "elapsed_time_sec" = $row.elapsed_time_sec
            "transaction_elapsed_time_sec" = $row.transaction_elapsed_time_sec
        }
        $ret = New-LineData $measurement $tags $fields $timestamp
        $data += $ret
    }
    Write-LineData -baseuri $baseuri -data $data

    $data = @()
    foreach($row in $ds.Tables["waitstats"].Rows){
        $measurement = "waitstats"
        $tags = @{
            "server_name"=($row.server_name -replace " ","\ ")
            "sql_instance_name" = ($row.sql_instance_name -replace " ","\ ")
            "database_name" = ($row.database_name -replace " ","\ ")
            "wait_category" = ($row.wait_category -replace " ","\ ")

        }

        $fields = @{
            "wait_time_ms" = $row.wait_time_ms
            "waiting_tasks_count" = $row.waiting_tasks_count
            "max_wait_time_ms" = $row.max_wait_time_ms
        }
        $ret = New-LineData $measurement $tags $fields $timestamp
        $data += $ret
    }
    Write-LineData -baseuri $baseuri -data $data

    $ds.Clear()
    $ds.Dispose()
    $ds = $null

    Start-Sleep -Seconds $sleep_interval
}

$da.Dispose()
$con.Close()
$con.Dispose()