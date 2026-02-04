# ТЕСТ С ИЗМЕРЕНИЕМ РЕАЛЬНЫХ ПОТЕРЬ (ИСПРАВЛЕННЫЙ)

function Test-RecoveryWithLoss {
    param([string]$configName, [int]$timeout, [int]$heartbeat)
    
    $groupName = "loss-$configName-$(Get-Date -Format 'HHmmss')"
    $topicName = "loss-$configName-$(Get-Date -Format 'HHmmss')"
    
    Write-Host "`nКонфигурация: $configName ($timeout/$heartbeat ms)"
    
    # 1. Подготовка
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic $topicName --partitions 3 --replication-factor 1 2>$null
    docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group $groupName 2>$null
    Start-Sleep -Seconds 2
    
    # 2. Запуск потребителей
    Write-Host "Запуск 3 потребителей..."
    1..3 | ForEach-Object {
        Start-Process powershell -ArgumentList @"
            -WindowStyle Minimized -Command `
            java -jar `"clients\target\clients-1.0-SNAPSHOT.jar`" experiment-consumer $_ sticky $timeout $heartbeat $groupName $topicName
"@
        Start-Sleep -Seconds 1
    }
    
    # 3. Дать потребителям запуститься
    Start-Sleep -Seconds 10
    
    # 4. Запуск продюсера
    Write-Host "Запуск продюсера (2000/сек)..."
    Start-Process powershell -ArgumentList @"
        -WindowStyle Minimized -Command `
        java -jar `"clients\target\clients-1.0-SNAPSHOT.jar`" experiment-producer 2000 60 $topicName
"@
    
    # 5. Накопление LAG
    Write-Host "Накопление LAG (25 секунд)..."
    Start-Sleep -Seconds 25
    
    # 6. Замерить LAG перед сбоем (ПРАВИЛЬНЫЙ ПАРСИНГ)
    $before = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --group $groupName --describe
    Write-Host "`nСостояние перед сбоем:"
    
    # Правильный парсинг LAG - берём 6-е поле (индекс 5)
    $totalLagBefore = 0
    $lines = $before -split "`n"
    foreach ($line in $lines) {
        $fields = $line -split '\s+'
        if ($fields.Count -ge 6 -and $fields[5] -match '^\d+$') {
            $lag = [int]$fields[5]
            $totalLagBefore += $lag
            Write-Host "  Partition $($fields[2]) LAG: $lag"
        }
    }
    Write-Host "Общий LAG перед сбоем: $totalLagBefore"
    
    # 7. Сбой
    Write-Host "`nЗакройте ОДНО окно с потребителем, затем Enter"
    Pause
    
    $startTime = Get-Date
    Write-Host "Мониторинг начат: $(Get-Date -Format 'HH:mm:ss')"
    
    $recoveryTime = $null
    $maxLagDuring = 0
    
    # 8. Мониторинг с ПРАВИЛЬНЫМ парсингом LAG
    for($i=1; $i -le 50; $i++) {
        $check = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --group $groupName --describe 2>$null
        
        if($check) {
            # Потребители
            $currentIds = ($check | Select-String "consumer-$groupName-\d+-([a-f0-9\-]+)" | 
                ForEach-Object { $_.Matches[0].Groups[1].Value } | 
                Sort-Object -Unique).Count
            
            # LAG (правильный парсинг)
            $currentTotalLag = 0
            $checkLines = $check -split "`n"
            foreach ($line in $checkLines) {
                $fields = $line -split '\s+'
                if ($fields.Count -ge 6 -and $fields[5] -match '^\d+$') {
                    $currentTotalLag += [int]$fields[5]
                }
            }
            
            if ($currentTotalLag -gt $maxLagDuring) { 
                $maxLagDuring = $currentTotalLag 
            }
            
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $i : $currentIds потребителей, LAG: $currentTotalLag"
            
            if($currentIds -eq 2) {
                $recoveryTime = (Get-Date) - $startTime
                Write-Host "Восстановление: $([math]::Round($recoveryTime.TotalSeconds, 1))с"
                
                # Ждём обработки
                Start-Sleep -Seconds 15
                break
            }
        }
        
        if($i -eq 50) {
            $recoveryTime = (Get-Date) - $startTime
            Write-Host "Не завершено за $([math]::Round($recoveryTime.TotalSeconds, 1))с"
        }
        
        Start-Sleep -Seconds 2
    }
    
    # 9. Замер после восстановления
    $after = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --group $groupName --describe
    Write-Host "`nСостояние после восстановления:"
    
    $totalLagAfter = 0
    $afterLines = $after -split "`n"
    foreach ($line in $afterLines) {
        $fields = $line -split '\s+'
        if ($fields.Count -ge 6 -and $fields[5] -match '^\d+$') {
            $totalLagAfter += [int]$fields[5]
        }
    }
    
    # 10. Расчёт потерь
    $lostMessages = $maxLagDuring - $totalLagBefore
    if ($lostMessages -lt 0) { $lostMessages = 0 }
    
    # 11. Очистка
    Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    
    # 12. Результат
    $result = [PSCustomObject]@{
        Config = $configName
        Timeout = $timeout
        Heartbeat = $heartbeat
        Recovery = if ($recoveryTime) { [math]::Round($recoveryTime.TotalSeconds, 1) } else { "N/A" }
        LagBefore = $totalLagBefore
        MaxLagDuring = $maxLagDuring
        LagAfter = $totalLagAfter
        Lost = $lostMessages
    }
    
    Write-Host "`nИтог:"
    Write-Host "  LAG до сбоя: $totalLagBefore"
    Write-Host "  Макс LAG во время сбоя: $maxLagDuring"
    Write-Host "  LAG после восстановления: $totalLagAfter"
    Write-Host "  Потери (дополнительный LAG): $lostMessages сообщений`n"
    
    return $result
}

# ЗАПУСК
Write-Host "Тест отказоустойчивости с измерением потерь"
Write-Host "==========================================="

Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3

$results = @()

Write-Host "`n1. Агрессивная (6с/2с)"
$results += Test-RecoveryWithLoss -configName "aggressive" -timeout 6000 -heartbeat 2000
Start-Sleep -Seconds 15

Write-Host "`n2. Балансная (10с/3с)"
$results += Test-RecoveryWithLoss -configName "balanced" -timeout 10000 -heartbeat 3000
Start-Sleep -Seconds 15

Write-Host "`n3. Консервативная (45с/15с)"
$results += Test-RecoveryWithLoss -configName "conservative" -timeout 45000 -heartbeat 15000

# Итоги
Write-Host "`nРЕЗУЛЬТАТЫ:"
Write-Host "============"

$results | Format-Table -Property `
    @{Name="Конфигурация";Expression={$_.Config}},
    @{Name="Восстановление(с)";Expression={$_.Recovery};Align="Right"},
    @{Name="LAG до";Expression={$_.LagBefore};Align="Right"},
    @{Name="Макс LAG";Expression={$_.MaxLagDuring};Align="Right"},
    @{Name="LAG после";Expression={$_.LagAfter};Align="Right"},
    @{Name="Потери";Expression={$_.Lost};Align="Right"} -AutoSize

# Анализ
Write-Host "`nАНАЛИЗ:"
foreach ($r in $results) {
    if ($r.Recovery -ne "N/A") {
        $lossPerSecond = [math]::Round($r.Lost / $r.Recovery, 0)
        Write-Host "$($r.Config): $($r.Lost) потерянных сообщений за $($r.Recovery)с (~$lossPerSecond/сек)"
    }
}