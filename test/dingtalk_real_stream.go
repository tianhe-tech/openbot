//go:build dingtalk_real_stream

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	clientID := strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_ID"))
	clientSecret := strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_SECRET"))
	userID := strings.TrimSpace(os.Getenv("DINGTALK_OWNER_USERID"))
	
	if clientID == "" || clientSecret == "" || userID == "" {
		fmt.Println("请设置：DINGTALK_CLIENT_ID, DINGTALK_CLIENT_SECRET, DINGTALK_OWNER_USERID")
		os.Exit(1)
	}
	
	ctx := context.Background()
	token := getAccessToken(ctx, clientID, clientSecret)
	fmt.Println("✓ Token 获取成功")
	
	outTrackID := fmt.Sprintf("stream-%d", time.Now().UnixNano())
	fmt.Printf("📱 卡片 ID: %s\n\n", outTrackID)
	
	fmt.Println("1️⃣ 创建卡片...")
	createCard(ctx, token, outTrackID)
	fmt.Println("   ✓")
	
	fmt.Println("2️⃣ 投放卡片...")
	deliverCard(ctx, token, outTrackID, userID, clientID)
	fmt.Println("   ✓")
	
	fmt.Println("3️⃣ 切换到 INPUTING...")
	switchToInputing(ctx, token, outTrackID)
	fmt.Println("   ✓")
	
	fmt.Println("4️⃣ 流式发送（模拟 Gateway SSE）...")
	content := "你好！这是一条真正的流式消息🚀\n\n你将看到文字逐字显示...\n\n1. 第一段内容\n2. 第二段内容\n3. 第三段内容\n\n✅ 完成！"
	
	// 模拟 SSE 流式：逐字累积发送
	streamingSend(ctx, token, outTrackID, content, 80)
	fmt.Println("   ✓")
	
	fmt.Println("5️⃣ 切换到 FINISHED...")
	switchToFinished(ctx, token, outTrackID, content)
	fmt.Println("   ✓")
	
	fmt.Println("\n✅ 真正的流式测试完成！请在钉钉查看打字机效果。")
}

func getAccessToken(ctx context.Context, clientID, clientSecret string) string {
	body := map[string]string{"appKey": clientID, "appSecret": clientSecret}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.dingtalk.com/v1.0/oauth2/accessToken", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var result struct{ AccessToken string `json:"accessToken"` }
	json.Unmarshal(raw, &result)
	return result.AccessToken
}

func createCard(ctx context.Context, token, outTrackID string) {
	body := map[string]interface{}{
		"cardTemplateId": "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema",
		"outTrackId": outTrackID,
		"cardData": map[string]interface{}{
			"cardParamMap": map[string]string{"config": `{"autoLayout":true}`},
		},
		"callbackType": "STREAM",
		"imRobotOpenSpaceModel": map[string]bool{"supportForward": true},
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.dingtalk.com/v1.0/card/instances", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, _ := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	defer resp.Body.Close()
}

func deliverCard(ctx context.Context, token, outTrackID, userID, robotCode string) {
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"userId": userID,
		"userIdType": 1,
		"openSpaceId": fmt.Sprintf("dtv1.card//IM_ROBOT.%s", userID),
		"imRobotOpenDeliverModel": map[string]string{"robotCode": robotCode},
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.dingtalk.com/v1.0/card/instances/deliver", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, _ := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	defer resp.Body.Close()
}

func switchToInputing(ctx context.Context, token, outTrackID string) {
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"cardData": map[string]interface{}{
			"cardParamMap": map[string]string{
				"flowStatus": "2",
				"msgContent": "",
				"staticMsgContent": "",
				"sys_full_json_obj": `{"order":["msgContent"]}`,
			},
		},
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.dingtalk.com/v1.0/card/instances", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, _ := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	defer resp.Body.Close()
}

func streamingSend(ctx context.Context, token, outTrackID, fullContent string, intervalMs int) {
	runes := []rune(fullContent)
	
	// 模拟 SSE：逐字累积发送（官方做法）
	for i := 1; i <= len(runes); i += 2 {
		if i > len(runes) {
			i = len(runes)
		}
		
		// 累积到当前位置的内容
		accumulated := string(runes[:i])
		
		body := map[string]interface{}{
			"outTrackId": outTrackID,
			"guid": fmt.Sprintf("s%d", i),
			"key": "msgContent",
			"content": accumulated,
			"isFull": true,  // 全量替换（关键！）
			"isFinalize": false,
		}
		data, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.dingtalk.com/v1.0/card/streaming", bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-acs-dingtalk-access-token", token)
		resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
		if err != nil {
			fmt.Printf("   错误：%v\n", err)
			return
		}
		resp.Body.Close()
		
		progress := i * 100 / len(runes)
		fmt.Printf("\r   进度：%3d%% (%d/%d) 已发送：%s", progress, i, len(runes), accumulated[max(0, len(accumulated)-30):])
		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
	}
	fmt.Println()
	
	// 最后一次标记 finalize=true
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"guid": "final",
		"key": "msgContent",
		"content": fullContent,
		"isFull": true,
		"isFinalize": true,
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.dingtalk.com/v1.0/card/streaming", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, _ := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	defer resp.Body.Close()
}

func switchToFinished(ctx context.Context, token, outTrackID, content string) {
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"cardData": map[string]interface{}{
			"cardParamMap": map[string]string{
				"flowStatus": "3",
				"msgContent": content,
				"staticMsgContent": content,
				"sys_full_json_obj": `{"order":["msgContent"]}`,
			},
			"cardUpdateOptions": map[string]bool{"updateCardDataByKey": true},
		},
	}
	data, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.dingtalk.com/v1.0/card/instances", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, _ := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	defer resp.Body.Close()
}
