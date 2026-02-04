# ТЕСТ НЕОДНОРОДНОЙ НАГРУЗКИ

function Test-HotspotStrategy {
    param([string]$strategy)
    
    $groupName = "hotspot-$strategy-$(Get-Date -Format 'HHmmss')"
    $topicName = "hotspot-$strategy-$(Get-Date -Format 'HHmmss')"
    
    Write-Host "`nТестируем стратегию: $strategy"
    Write-Host "Группа: $groupName"
    
    # 1. Подготовка
    docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic $topicName --partitions 12 --replication-factor 1 2>$null
    docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group $groupName 2>$null
    Start-Sleep -Seconds 2
    
    # 2. Запуск продюсера с hotspot (80/20)
    Write-Host "Запуск продюсера с hotspot (80/20 распределение)..."
    Start-Process powershell -ArgumentList @"
        -WindowStyle Minimized -Command `
        java -jar `"clients\target\clients-1.0-SNAPSHOT.jar`" experiment-producer 5000 300 $topicName --hotspot
"@
    
    # 3. Запуск 4 потребителей
    Write-Host "Запуск 4 потребителей..."
    1..4 | ForEach-Object {
        Start-Process powershell -ArgumentList @"
            -WindowStyle Minimized -Command `
            java -jar `"clients\target\clients-1.0-SNAPSHOT.jar`" experiment-consumer $_ $strategy 10000 3000 $groupName $topicName
"@
        Start-Sleep -Seconds 2
    }
    
    # 4. Мониторинг распределения
    Write-Host "`nМониторинг распределения (каждые 30 секунд):"
    
    $measurements = @()
    for($i=1; $i -le 6; $i++) {
        Start-Sleep -Seconds 30
        
        $result = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --group $groupName --describe
        
        # Анализ распределения сообщений
        $messagesByConsumer = @{}
        $resultLines = $result -split "`n"
        
        foreach ($line in $resultLines) {
            $fields = $line -split '\s+'
            if ($fields.Count -ge 7) {
                $consumerMatch = $line | Select-String "consumer-$groupName-\d+-([a-f0-9\-]+)"
                if ($consumerMatch) {
                    $consumerId = $consumerMatch.Matches[0].Groups[1].Value
                    if ($fields[3] -match '^\d+$') {
                        $messages = [int]$fields[3]
                        if (-not $messagesByConsumer.ContainsKey($consumerId)) {
                            $messagesByConsumer[$consumerId] = 0
                        }
                        $messagesByConsumer[$consumerId] += $messages
                    }
                }
            }
        }
        
        # Расчёт дисбаланса
        $totalMessages = ($messagesByConsumer.Values | Measure-Object -Sum).Sum
        $avgMessages = $totalMessages / $messagesByConsumer.Count
        $maxDeviation = 0
        
        foreach ($msgCount in $messagesByConsumer.Values) {
            $deviation = [Math]::Abs($msgCount - $avgMessages) / $avgMessages * 100
            if ($deviation -gt $maxDeviation) {
                $maxDeviation = $deviation
            }
        }
        
        $measurements += [PSCustomObject]@{
            Time = $i * 30
            Imbalance = [math]::Round($maxDeviation, 1)
            Consumers = $messagesByConsumer.Count
            Distribution = ($messagesByConsumer.Values | Sort-Object) -join ","
        }
        
        Write-Host "  Через $($i*30)с: дисбаланс $([math]::Round($maxDeviation, 1))%, распределение: $($measurements[-1].Distribution)"
    }
    
    # 5. Тест "горячей точки" (только для Dynamic)
    if ($strategy -eq "dynamic") {
        Write-Host "`nТест адаптивности: создание горячей точки..."
        
        # Запоминаем текущее распределение
        $beforeHotspot = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --group $groupName --describe
        
        # Создаём hotspot вручную (можно через отдельный продюсер в один раздел)
        Write-Host "  Запускаем дополнительный продюсер в раздел 0..."
        $hotspotProducer = Start-Process powershell -ArgumentList @"
            -WindowStyle Minimized -Command `
            java -jar `"clients\target\clients-1.0-SNAPSHOT.jar`" experiment-producer 2000 60 $topicName
"@ -PassThru
        
        # Мониторим выравнивание
        $hotspotStart = Get-Date
        $normalizationTime = $null
        
        for($j=1; $j -le 20; $j++) {
            Start-Sleep -Seconds 10
            
            $current = docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --group $groupName --describe
            
            # Проверяем баланс
            $messagesByConsumer = @{}
            $currentLines = $current -split "`n"
            
            foreach ($line in $currentLines) {
                $fields = $line -split '\s+'
                if ($fields.Count -ge 7) {
                    $consumerMatch = $line | Select-String "consumer-$groupName-\d+-([a-f0-9\-]+)"
                    if ($consumerMatch) {
                        $consumerId = $consumerMatch.Matches[0].Groups[1].Value
                        if ($fields[3] -match '^\d+$') {
                            $messages = [int]$fields[3]
                            if (-not $messagesByConsumer.ContainsKey($consumerId)) {
                                $messagesByConsumer[$consumerId] = 0
                            }
                            $messagesByConsumer[$consumerId] += $messages
                        }
                    }
                }
            }
            
            $maxMsg = ($messagesByConsumer.Values | Measure-Object -Maximum).Maximum
            $minMsg = ($messagesByConsumer.Values | Measure-Object -Minimum).Minimum
            $ratio = $maxMsg / $minMsg
            
            Write-Host "  Через $($j*10)с: соотношение max/min = $([math]::Round($ratio, 2))"
            
            if ($ratio -lt 1.5) {
                $normalizationTime = (Get-Date) - $hotspotStart
                Write-Host "  Выравнивание за $([math]::Round($normalizationTime.TotalSeconds, 1))с"
                break
            }
            
            if ($j -eq 20) {
                $normalizationTime = (Get-Date) - $hotspotStart
                Write-Host "  Не выровнялось за $([math]::Round($normalizationTime.TotalSeconds, 1))с"
            }
        }
        
        Stop-Process -Id $hotspotProducer.Id -Force -ErrorAction SilentlyContinue
    }
    
    # 6. Очистка
    Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3
    
    # 7. Результаты
    $avgImbalance = ($measurements.Imbalance | Measure-Object -Average).Average
    
    $result = [PSCustomObject]@{
        Strategy = $strategy
        AvgImbalance = [math]::Round($avgImbalance, 1)
        FinalImbalance = $measurements[-1].Imbalance
        Distribution = $measurements[-1].Distribution
        NormalizationTime = if ($normalizationTime) { [math]::Round($normalizationTime.TotalSeconds, 1) } else { "N/A" }
    }
    
    Write-Host "Средний дисбаланс: $([math]::Round($avgImbalance, 1))%`n"
    return $result
}

# ЗАПУСК
Write-Host "ТЕСТ 3: НЕОДНОРОДНАЯ НАГРУЗКА"
Write-Host "=============================="
Write-Host "Hotspot: 80% в 4 раздела, 20% в 8 разделов"
Write-Host "Продюсер: 5000/сек, Потребители: 4, Разделы: 12`n"

Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 5

$results = @()

Write-Host "1. Range"
$results += Test-HotspotStrategy -strategy "range"
Start-Sleep -Seconds 10

Write-Host "`n2. RoundRobin"
$results += Test-HotspotStrategy -strategy "roundrobin"
Start-Sleep -Seconds 10

Write-Host "`n3. Sticky"
$results += Test-HotspotStrategy -strategy "sticky"
Start-Sleep -Seconds 10

Write-Host "`n4. Dynamic"
$results += Test-HotspotStrategy -strategy "dynamic"

# Итоги
Write-Host "`nРЕЗУЛЬТАТЫ ЭКСПЕРИМЕНТА 3:"
Write-Host "============================"

$results | Format-Table -Property `
    @{Name="Стратегия";Expression={$_.Strategy}},
    @{Name="Ср.дисбаланс%";Expression={$_.AvgImbalance};Align="Right"},
    @{Name="Фин.дисбаланс%";Expression={$_.FinalImbalance};Align="Right"},
    @{Name="Распределение";Expression={$_.Distribution}},
    @{Name="Выравнивание(с)";Expression={$_.NormalizationTime};Align="Right"} -AutoSize

# Анализ
Write-Host "`nАНАЛИЗ:"
Write-Host "1. Range: статическое распределение, не адаптируется к нагрузке"
Write-Host "2. RoundRobin: равномерное распределение разделов, но не нагрузки"
Write-Host "3. Sticky: минимальные перемещения, но фиксированное распределение"
Write-Host "4. Dynamic: потенциальная адаптивность (если реализована)"