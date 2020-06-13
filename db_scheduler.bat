set exe_path=%cd%\build\main_db.exe
SchTasks /Create /SC MINUTE /MO 5 /TN IES_DB_Update /TR "%exe_path%"

