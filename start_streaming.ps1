# Jalankan pipeline Kafka + Spark Streaming (Windows PowerShell)
# Usage: .\start_streaming.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Kafka + Spark Streaming Pipeline" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$ProjectRoot = $PSScriptRoot
Set-Location $ProjectRoot

# 1. Start Kafka via Docker
Write-Host "`n[1/3] Starting Kafka..." -ForegroundColor Yellow
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "Gagal start Kafka. Pastikan Docker Desktop berjalan." -ForegroundColor Red
    exit 1
}

Write-Host "Menunggu Kafka siap (15 detik)..." -ForegroundColor Gray
Start-Sleep -Seconds 15

# 2. Start Spark Streaming di terminal baru
Write-Host "`n[2/3] Starting Spark Streaming consumer..." -ForegroundColor Yellow
$sparkCmd = "Set-Location '$ProjectRoot'; python spark_streaming.py"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $sparkCmd

# 3. Start Kafka Producer di terminal baru
Write-Host "`n[3/3] Starting Kafka Producer (replay mode)..." -ForegroundColor Yellow
$producerCmd = "Set-Location '$ProjectRoot'; python kafka_producer.py --delay 0.05"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $producerCmd

Write-Host "`nPipeline berjalan di 2 terminal terpisah." -ForegroundColor Green
Write-Host "Buka dashboard: streamlit run app.py" -ForegroundColor Green
Write-Host "Tab: Real-time Streaming" -ForegroundColor Green
