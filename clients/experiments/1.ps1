# ============================================
# ТЕСТ ВСЕХ СТРАТЕГИЙ РАСПРЕДЕЛЕНИЯ
# Сравнение Range, RoundRobin, Sticky, Dynamic
# ============================================

function Test-Strategy {
    param([string]$strategy)
    
    $groupName = test-$strategy-$(Get-Date -Format 'HHmmss')
    $topicName = topic-$strategy-$(Get-Date -Format 'HHmmss')
    
    Write-Host Тестируем стратегию $strategy
    Write-Host Группа $groupName
    
    # 1. Подготовка
    docker exec kafka-1 kafka-topics --bootstrap-server localhost9092 --create --topic $topicName --partitions 12 --replication-factor 1 2$null
    docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost9092 --delete --group $groupName 2$null
    Start-Sleep -Seconds 2
    
    # 2. Запуск продюсера
    $producerJob = Start-Job -ScriptBlock {
        param($topic)
        java -jar clientstargetclients-1.0-SNAPSHOT.jar experiment-producer 5000 120 $topic
    } -ArgumentList $topicName
    
    # 3. Запуск 4 потребителей
    1..4  ForEach-Object {
        Start-Process powershell -ArgumentList @
            -WindowStyle Minimized -Command `
            java -jar `clientstargetclients-1.0-SNAPSHOT.jar` experiment-consumer $_ $strategy 6000 2000 $groupName $topicName
@
        Start-Sleep -Seconds 1
    }
    
    # 4. Ожидание стабилизации
    Start-Sleep -Seconds 30
    
    # 5. Анализ распределения
    $initialResult = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost9092 --group $groupName --describe
    
    $partitionsByConsumer = @{}
    $initialResult  Select-String consumer-$groupName-d+-([a-f0-9-]+)  ForEach-Object {
        $consumerId = $_.Matches[0].Groups[1].Value
        $partition = ($_.Line -split 's+')[2]
        if (-not $partitionsByConsumer.ContainsKey($consumerId)) {
            $partitionsByConsumer[$consumerId] = @()
        }
        $partitionsByConsumer[$consumerId] += $partition
    }
    
    Write-Host Распределение $($partitionsByConsumer.Count) потребителей
    
    # 6. Рассчёт метрик
    Start-Sleep -Seconds 30
    
    $metricsResult = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost9092 --group $groupName --describe
    
    $messagesByConsumer = @{}
    $totalMessages = 0
    $metricsResult  Select-String consumer-$groupName-d+-([a-f0-9-]+)  ForEach-Object {
        $consumerId = $_.Matches[0].Groups[1].Value
        $currentOffset = [int](($_.Line -split 's+')[3])
        if (-not $messagesByConsumer.ContainsKey($consumerId)) {
            $messagesByConsumer[$consumerId] = 0
        }
        $messagesByConsumer[$consumerId] += $currentOffset
        $totalMessages += $currentOffset
    }
    
    # Дисбаланс
    $avgMessages = $totalMessages  $messagesByConsumer.Count
    $maxDeviation = 0
    $messagesByConsumer.Values  ForEach-Object {
        $deviation = [Math]Abs($_ - $avgMessages)  $avgMessages  100
        if ($deviation -gt $maxDeviation) {
            $maxDeviation = $deviation
        }
    }
    
    $throughput = [math]Round($totalMessages  60, 0)
    
    # 7. Тест отказоустойчивости
    Write-Host Закройте одно окно с потребителем, затем нажмите Enter
    Pause
    
    $recoveryStart = Get-Date
    $recoveryTime = $null
    
    for($i=1; $i -le 15; $i++) {
        $check = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost9092 --group $groupName --describe 2$null
        
        if($check) {
            $currentIds = ($check  Select-String consumer-$groupName-d+-([a-f0-9-]+)  
                ForEach-Object { $_.Matches[0].Groups[1].Value }  
                Sort-Object -Unique).Count
            
            if($currentIds -eq 3) {
                $recoveryTime = (Get-Date) - $recoveryStart
                Write-Host Восстановление $([math]Round($recoveryTime.TotalSeconds, 1))с
                break
            }
        }
        
        if($i -eq 15) {
            $recoveryTime = (Get-Date) - $recoveryStart
            Write-Host Восстановление не завершено за $([math]Round($recoveryTime.TotalSeconds, 1))с
        }
        
        Start-Sleep -Seconds 2
    }
    
    # 8. Очистка
    Stop-Job $producerJob -Force 2$null
    Remove-Job $producerJob -Force 2$null
    taskkill F IM java.exe 2$null
    
    # 9. Результаты
    $result = [PSCustomObject]@{
        Strategy = $strategy
        Throughput = $throughput
        Imbalance = [math]Round($maxDeviation, 1)
        RecoveryTime = if ($recoveryTime) { [math]Round($recoveryTime.TotalSeconds, 1) } else { NA }
        Distribution = ($partitionsByConsumer.Values  ForEach-Object { $_.Count }) -join ,
    }
    
    Write-Host Throughput ${throughput}сек, Дисбаланс $([math]Round($maxDeviation, 1))%`n
    
    return $result
}

# ============ ОСНОВНОЙ СКРИПТ ============

Write-Host Сравнение стратегий распределения
Write-Host Параметры 4 потребителя, 12 разделов, продюсер 5000сек`n

# Очистка старых процессов
taskkill F IM java.exe 2$null
Start-Sleep -Seconds 3

# Тестируем стратегии
$results = @()

Write-Host --- Range ---
$results += Test-Strategy -strategy range
Start-Sleep -Seconds 5

Write-Host --- RoundRobin ---
$results += Test-Strategy -strategy roundrobin
Start-Sleep -Seconds 5

Write-Host --- Sticky ---
$results += Test-Strategy -strategy sticky
Start-Sleep -Seconds 5

Write-Host --- Dynamic ---
$results += Test-Strategy -strategy dynamic

# Итоговая таблица
Write-Host `nИтоговые результаты
Write-Host -------------------

$results  Format-Table -Property `
    @{Name=Стратегия;Expression={$_.Strategy}},
    @{Name=Throughput;Expression={$_.Throughput};Align=Right},
    @{Name=Дисбаланс%;Expression={$_.Imbalance};Align=Right},
    @{Name=Восст.(с);Expression={$_.RecoveryTime};Align=Right},
    @{Name=Распределение;Expression={$_.Distribution}} -AutoSize

# Анализ
Write-Host `nАнализ

$bestThroughput = $results  Sort-Object Throughput -Descending  Select-Object -First 1
Write-Host Лучшая производительность $($bestThroughput.Strategy) ($($bestThroughput.Throughput)сек)

$bestBalance = $results  Sort-Object Imbalance  Select-Object -First 1
Write-Host Лучший баланс $($bestBalance.Strategy) ($($bestBalance.Imbalance)%)

$bestRecovery = $results  Where-Object { $_.RecoveryTime -ne NA }  Sort-Object RecoveryTime  Select-Object -First 1
Write-Host Лучшее восстановление $($bestRecovery.Strategy) ($($bestRecovery.RecoveryTime)с)