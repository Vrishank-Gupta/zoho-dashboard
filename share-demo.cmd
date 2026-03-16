@echo off
setlocal
cd /d "%~dp0"
echo Starting public tunnel for http://127.0.0.1:8010
echo Keep this window open while sharing the demo.
npx localtunnel --port 8010
