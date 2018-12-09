[CmdletBinding()]
Param(
    $Zaiba2HOME = "C:\Zaiba2",
    $Zaiba2Script_uri = "https://raw.githubusercontent.com/MasayukiOzawa/Zaiba2/master/Scripts/Zaiba2.ps1",
    $Zaiba2DashBoard_uri = "https://raw.githubusercontent.com/MasayukiOzawa/Zaiba2/master/Chronograf/SQL%20Server%20Monitoring%20Dashboard%20_Zaiba2_.json",
    $Nawaboard_uri = "https://raw.githubusercontent.com/MasayukiOzawa/Zaiba2/master/Chronograf/SQL%20Database%20Data%20Access%20Dashboard%20_Nawa%20Board_.json",
    $influxdb_uri = "https://dl.influxdata.com/influxdb/releases/influxdb-1.7.1_windows_amd64.zip",
    $chronograf_uri = "https://dl.influxdata.com/chronograf/releases/chronograf-1.7.3_windows_amd64.zip",
    $Kapacitor_uri = "https://dl.influxdata.com/kapacitor/releases/kapacitor-1.5.1_windows_amd64.zip"
)
$ErrorActionPreference = "Stop"

Function Write-Zaiba2Log($Message){
    Write-Host ("{0} : {1}" -f (Get-Date), $Message)
}

# Make Dir Zaiba2HOME and Tools (Default : C:\Zaiba2)
Write-Zaiba2Log ("Create Zaiba2HOME ({0})" -f $Zaiba2HOME)

If (!(Test-Path $Zaiba2HOME)){
    New-Item -ItemType Directory -Path $Zaiba2HOME > $null
}
$tools_dir = (Join-Path $Zaiba2HOME "Tools")
If (!(Test-Path $tools_dir)){
    New-Item -ItemType Directory -Path $tools_dir > $null
}

# Download Zaiba2 Module
Write-Zaiba2Log ("Download the module of Zaiba2 from Github. ({0})" -f "Zaiba2.ps1, Zaiba2.json, NawaBoard.json")
(Invoke-WebRequest -Uri $Zaiba2Script_uri).Content | % { [Text.Encoding]::UTF8.GetBytes($_) } | Set-Content -Path (Join-Path $tools_dir "Zaiba2.ps1") -Encoding Byte
(Invoke-WebRequest -Uri $Zaiba2DashBoard_uri).Content | % { [Text.Encoding]::UTF8.GetBytes($_) } | Set-Content -Path (Join-Path $tools_dir "Zaiba2.json") -Encoding Byte
(Invoke-WebRequest -Uri $Nawaboard_uri).Content | % { [Text.Encoding]::UTF8.GetBytes($_) } | Set-Content -Path (Join-Path $tools_dir "NawaBoard.json") -Encoding Byte

# Download Tick Stack
Write-Zaiba2Log ("Download InfluxDB")
$influxdb_file = Join-Path  $Zaiba2HOME ($influxdb_uri -split "/")[-1]
Invoke-WebRequest -Uri $influxdb_uri -OutFile $influxdb_file

Write-Zaiba2Log ("Download Chronograf")
$chronograf_file = Join-Path $Zaiba2HOME ($chronograf_uri -split "/")[-1]
Invoke-WebRequest -Uri $chronograf_uri -OutFile $chronograf_file

Write-Zaiba2Log ("Download Kapacitor")
$kapacitor_file = Join-Path $Zaiba2HOME ($kapacitor_uri -split "/")[-1]
Invoke-WebRequest -Uri $kapacitor_uri -OutFile $kapacitor_file


# Expand Zip File
Write-Zaiba2Log ("Expand TickStack Module")
Expand-Archive -Path $influxdb_file -DestinationPath $Zaiba2HOME
Expand-Archive -Path $chronograf_file -DestinationPath $Zaiba2HOME
Expand-Archive -Path $kapacitor_file -DestinationPath $Zaiba2HOME

# Convert Unix Path in influxdb.conf
$influxdb_dir = Get-Item (Join-Path $Zaiba2HOME "influxdb*") | ? {$_.PSIsContainer -eq $true}
Write-Zaiba2Log ("Convert Linux path to Windows path ({0} -> {1})" -f "/var/lib/influxdb/", ($influxdb_dir.FullName))

$influxdb_conf_file = (Join-Path $(Join-Path $Zaiba2HOME $influxdb_dir.Name) "influxdb.conf")

Copy-Item $influxdb_conf_file -Destination (Join-Path $influxdb_dir "influxdb.conf.bak")
$conf = Get-Content $influxdb_conf_file
$conf -replace "/var/lib/influxdb/", (($influxdb_dir.FullName + "\") -replace "\\", "\\") | Out-File -FilePath $influxdb_conf_file -Force -Encoding utf8

# Convert Unix Path in kapacitor.conf
$kapacitor_dir = Get-Item (Join-Path $Zaiba2HOME "kapacitor*") | ? {$_.PSIsContainer -eq $true}
Write-Zaiba2Log ("Convert Linux path to Windows path ({0} -> {1})" -f "/var/log/kapacitor/", ($kapacitor_dir.FullName))

$kapacitor_conf_file = (Join-Path $(Join-Path $Zaiba2HOME $kapacitor_dir.Name) "kapacitor.conf")

Copy-Item $kapacitor_conf_file -Destination (Join-Path $kapacitor_dir "kapacitor.conf.bak")

$conf = Get-Content $kapacitor_conf_file
$conf = $conf -replace "/var/lib/kapacitor", (($kapacitor_dir.FullName + "\") -replace "\\", "\\") 
$conf = $conf -replace "/etc/kapacitor/load", (($kapacitor_dir.FullName + "\") -replace "\\", "\\") 
$conf = $conf -replace "/var/log/kapacitor/", (($kapacitor_dir.FullName + "\") -replace "\\", "\\") 
$conf = $conf -replace "/var/lib/influxdb/", (($influxdb_dir.FullName + "\") -replace "\\", "\\") 
$conf = $conf -replace "\\/", "\"
# If BOM is added, config is not recognized, BOM deletion
$conf | Out-String | % {[Text.Encoding]::UTF8.GetBytes($_)} | Set-Content -Path $kapacitor_conf_file -Encoding Byte

# Start Influxd
$influxd = (Join-Path $influxdb_dir "influxd.exe")
Write-Zaiba2Log ("Create Zaiba2 in InfluxDB")
$influxdb_job = Start-Job {Invoke-Expression "$($args[0]) -config $($args[1])"} -ArgumentList $influxd, $influxdb_conf_file
while ((Get-Process "influxd" -ErrorAction "SilentlyContinue") -eq $null){
    Start-Sleep -Seconds 1
}

# Create Database zaiba2
Invoke-Expression "$(Join-Path $influxdb_dir "influx.exe") -execute 'create database zaiba2'"

# Stop Influxd
Stop-Job $influxdb_job
$influxdb_job | Remove-Job

# Display start command
$chronograf = Get-Item (Join-Path $Zaiba2HOME "chronograf*\chronograf.exe")
$kapacitor =  Get-Item (Join-Path $Zaiba2HOME "kapacitor*\kapacitord.exe")

Write-Zaiba2Log "Setup is completed."
Write-Host ("=" * 10)

Write-Host "Execute the following command to start InfluxDB and Chronograf."
Write-Host "Please start two command prompts and execute the command at each."
Write-Host ("=" * 10)

Write-Host "Commands for starting InfluxDB."
Write-Host ("{0} -config {1} > nul 2>&1" -f $influxd, $influxdb_conf_file)
Write-Host ("=" * 10)

Write-Host "Commands for starting Chronograf."
Write-Host ("{0} > nul 2>&1" -f $chronograf)
Write-Host ("=" * 10)

Write-Host "Commands for starting Kapacitor."
Write-Host ("{0} -config {1} > nul 2>&1" -f $kapacitor, $kapacitor_conf_file)
Write-Host ("=" * 10)

Write-Host "PowerShell command for metrics collection."
Write-Host ("{0}\Zaiba2.ps1 -mssql_datasource ""<Server Name or IP>"" -mssql_userid ""<SQL Login>"" -mssql_password ""<Login Password>""" -f $tools_dir)

Write-Host "PowerShell command for metrics collection. (Azure SQL Databaes)"
Write-Host ("{0}\Zaiba2.ps1 -mssql_datasource ""<Server Name or IP>"" -mssql_userid ""<SQL Login>"" -mssql_password ""<Login Password>"" -mssql_initialcatalog ""<Database Name>"" -AzureSQLDB" -f $tools_dir)