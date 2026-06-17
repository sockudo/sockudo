@if '%1' == '' GOTO EXIT_WITH_ERROR

@set "WIP_FILE=C:\Program Files (x86)\Microsoft Visual Studio\2019\Enterprise\Common7\Tools\VsDevCmd.bat"
@if exist %WIP_FILE% (
    @call "%WIP_FILE%"
    GOTO GENERATE_SNK
)

@set "WIP_FILE=%VS150COMNTOOLS%\vsvars32.bat"
@if exist "%WIP_FILE%" (
    @call "%WIP_FILE%"
    GOTO GENERATE_SNK
)

@set "WIP_FILE=%VS140COMNTOOLS%\vsvars32.bat"
@if exist %WIP_FILE% (
    @call "%WIP_FILE%"
    GOTO GENERATE_SNK
)

@set "WIP_FILE=%VS130COMNTOOLS%\vsvars32.bat"
@if exist %WIP_FILE% (
    @call "%WIP_FILE%"
    GOTO GENERATE_SNK
)

@set "WIP_FILE=%VS120COMNTOOLS%\vsvars32.bat"
@if exist %WIP_FILE% (
    @call "%WIP_FILE%"
    GOTO GENERATE_SNK
)

@set "WIP_FILE=%VS110COMNTOOLS%\vsvars32.bat"
@if exist %WIP_FILE% (
    @call "%WIP_FILE%"
    GOTO GENERATE_SNK
)

@set "WIP_FILE=%VS100COMNTOOLS%\vsvars32.bat"
@if exist %WIP_FILE% (
    @call "%WIP_FILE%"
    GOTO GENERATE_SNK
)

:GENERATE_SNK
sn -k "%~1.snk"
sn -p "%~1.snk" "%~1.public.snk"
sn -tp "%~1.public.snk"
exit /B 0

:EXIT_WITH_ERROR
@echo Please provide a strong name key file name as a parameter to this command file.
