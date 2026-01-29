# Interactive API Test - Manual input with real-time I/O display
param(
    [Parameter()]
    [ValidateSet("feishu", "wecom", "dingtalk")]
    [string]$Adapter = "feishu"
)

$baseUrl = "http://localhost:8080"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Interactive Gateway API Tester" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Current Adapter: " -NoNewline
Write-Host $Adapter.ToUpper() -ForegroundColor Yellow
Write-Host "Endpoint: " -NoNewline
Write-Host "$baseUrl/$Adapter/callback" -ForegroundColor Gray
Write-Host ""
Write-Host "Type your message and press Enter to send." -ForegroundColor Gray
Write-Host "Type 'exit' or 'quit' to stop." -ForegroundColor Gray
Write-Host "Type 'switch' to change adapter." -ForegroundColor Gray
Write-Host ""

$sessionHistory = @{}

function Send-Message {
    param(
        [string]$AdapterType,
        [string]$Message
    )
    
    $timestamp = Get-Date -Format "HH:mm:ss"
    
    # Build payload based on adapter type
    $payload = switch ($AdapterType) {
        "feishu" {
            @{
                schema = "2.0"
                token = "test_token"
                type = "im.message.receive_v1"
                event = @{
                    sender = @{
                        sender_id = @{
                            open_id = "ou_interactive_user"
                            user_id = "user_interactive"
                        }
                    }
                    message = @{
                        message_id = "om_msg_$(Get-Random -Maximum 999999)"
                        message_type = "text"
                        chat_id = "oc_interactive_chat"
                        content = "{`"text`": `"$Message`"}"
                    }
                }
            }
        }
        "wecom" {
            @{
                msgtype = "text"
                from_userid = "interactive_user"
                text = @{
                    content = $Message
                }
            }
        }
        "dingtalk" {
            @{
                msgtype = "text"
                conversationType = "1"
                conversationId = "cid_interactive"
                senderStaffId = "staff_interactive"
                text = @{
                    content = $Message
                }
            }
        }
    }
    
    $jsonPayload = $payload | ConvertTo-Json -Depth 10 -Compress
    
    Write-Host "[$timestamp] " -NoNewline -ForegroundColor DarkGray
    Write-Host "YOU > " -NoNewline -ForegroundColor Green
    Write-Host $Message -ForegroundColor White
    
    Write-Host "[$timestamp] " -NoNewline -ForegroundColor DarkGray
    Write-Host "REQUEST > " -NoNewline -ForegroundColor Cyan
    Write-Host "Sending to /$AdapterType/callback..." -ForegroundColor Gray
    
    try {
        $response = Invoke-RestMethod -Uri "$baseUrl/$AdapterType/callback" -Method Post -Body $jsonPayload -ContentType "application/json" -ErrorAction Stop
        
        # Extract reply
        $replyText = $null
        if ($response.reply) {
            $replyText = $response.reply
        } elseif ($response.content -and $response.content.text) {
            $replyText = $response.content.text
        } elseif ($response.text -and $response.text.content) {
            $replyText = $response.text.content
        }
        
        Write-Host "[$timestamp] " -NoNewline -ForegroundColor DarkGray
        Write-Host "RESPONSE < " -NoNewline -ForegroundColor Yellow
        Write-Host "Session: $($response.session_id)" -ForegroundColor Gray
        
        Write-Host "[$timestamp] " -NoNewline -ForegroundColor DarkGray
        Write-Host "AI > " -NoNewline -ForegroundColor Magenta
        Write-Host $replyText -ForegroundColor White
        
        # Store session
        if ($response.session_id) {
            $sessionHistory[$AdapterType] = $response.session_id
        }
        
        Write-Host ""
        return $true
        
    } catch {
        Write-Host "[$timestamp] " -NoNewline -ForegroundColor DarkGray
        Write-Host "ERROR < " -NoNewline -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        Write-Host ""
        return $false
    }
}

# Main loop
while ($true) {
    Write-Host "[$Adapter] " -NoNewline -ForegroundColor Yellow
    Write-Host "> " -NoNewline -ForegroundColor Green
    
    $input = Read-Host
    
    if ([string]::IsNullOrWhiteSpace($input)) {
        continue
    }
    
    $input = $input.Trim()
    
    if ($input -eq "exit" -or $input -eq "quit") {
        Write-Host ""
        Write-Host "Goodbye!" -ForegroundColor Cyan
        break
    }
    
    if ($input -eq "switch") {
        Write-Host ""
        Write-Host "Select adapter:" -ForegroundColor Cyan
        Write-Host "1. Feishu (飞书)" -ForegroundColor Gray
        Write-Host "2. WeCom (企业微信)" -ForegroundColor Gray
        Write-Host "3. DingTalk (钉钉)" -ForegroundColor Gray
        Write-Host -NoNewline "Choice [1-3]: "
        
        $choice = Read-Host
        
        $Adapter = switch ($choice) {
            "1" { "feishu" }
            "2" { "wecom" }
            "3" { "dingtalk" }
            default { $Adapter }
        }
        
        Write-Host ""
        Write-Host "Switched to: " -NoNewline
        Write-Host $Adapter.ToUpper() -ForegroundColor Yellow
        Write-Host ""
        continue
    }
    
    if ($input -eq "history") {
        Write-Host ""
        Write-Host "Session History:" -ForegroundColor Cyan
        foreach ($key in $sessionHistory.Keys) {
            Write-Host "  $key : " -NoNewline -ForegroundColor Gray
            Write-Host $sessionHistory[$key] -ForegroundColor Yellow
        }
        Write-Host ""
        continue
    }
    
    if ($input -eq "help") {
        Write-Host ""
        Write-Host "Commands:" -ForegroundColor Cyan
        Write-Host "  exit, quit  - Exit the program" -ForegroundColor Gray
        Write-Host "  switch      - Change adapter" -ForegroundColor Gray
        Write-Host "  history     - Show session IDs" -ForegroundColor Gray
        Write-Host "  help        - Show this help" -ForegroundColor Gray
        Write-Host ""
        Write-Host "Or type any message to send to OpenCode" -ForegroundColor Gray
        Write-Host ""
        continue
    }
    
    # Send message
    Send-Message -AdapterType $Adapter -Message $input
}
