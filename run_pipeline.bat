@echo off
setlocal

echo Auto Compare Runner
echo.
echo Paste your mobile.de search URL:
set /p SEARCH_URL=

if "%SEARCH_URL%"=="" (
  echo.
  echo No URL provided. Exiting.
  exit /b 1
)

echo.
echo Running scrape + clean...
python run_pipeline.py "%SEARCH_URL%"

echo.
echo Finished. You can now upload data\processed\cars_clean.parquet into the Streamlit app.
pause

