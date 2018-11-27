echo checking internet connection
Ping www.google.com -n 1 -w 1000
cls
if errorlevel 1 (goto sync_locations) else (echo "Not Connected to Internet -- Skipped")

:sync_locations
  cls
  echo "Conneted to Internet -- Running Proc"
  cmd /c C:\"Program Files"\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe "C:\Users\timothy.carambat\Desktop\SF_loc_Auto_Deploy\sf_sync.py"
