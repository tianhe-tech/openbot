//go:build dingtalk_aicard_check

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	tokenAPI      = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
	cardAPI       = "https://api.dingtalk.com/v1.0/card/instances"
	deliverAPI    = "https://api.dingtalk.com/v1.0/card/instances/deliver"
	streamingAPI  = "https://api.dingtalk.com/v1.0/card/streaming"
)

const aiCardTemplateID = "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema"

func main() {
	clientID := strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_ID"))
	clientSecret := strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_SECRET"))

	userID := flag.String("userId", strings.TrimSpace(os.Getenv("DINGTALK_OWNER_USERID")), "用户 ID")
	content := flag.String("content", "测试消息", "卡片内容")
	flag.Parse()

	if clientID == "" || clientSecret == "" {
		fmt.Println("缺少 DINGTALK_CLIENT_ID 或 DINGTALK_CLIENT_SECRET")
		os.Exit(1)
	}

	ctx := context.Background()
	
	// 1. 获取 token
	token, err := getAccessToken(ctx, clientID, clientSecret)
	if err != nil {
		fmt.Printf("获取 token 失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Println("✓ Token 获取成功")

	// 2. 创建卡片（只设置 config）
	outTrackID := fmt.Sprintf("card-%d", time.Now().UnixNano())
	err = createCard(ctx, token, outTrackID)
	if err != nil {
		fmt.Printf("创建卡片失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✓ 卡片创建成功：%s\n", outTrackID)

	// 3. 投放卡片
	err = deliverCard(ctx, token, outTrackID, *userID, clientID)
	if err != nil {
		fmt.Printf("投放失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✓ 卡片已投放到用户：%s\n", *userID)

	// 4. 切换到 INPUTING 状态
	err = switchToInputing(ctx, token, outTrackID)
	if err != nil {
		fmt.Printf("切换状态失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Println("✓ 切换到 INPUTING 状态")

	// 5. 流式更新内容
	err = streamUpdate(ctx, token, outTrackID, *content, true)
	if err != nil {
		fmt.Printf("流式更新失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✓ 内容已更新（长度：%d）\n", len(*content))

	// 6. 切换到 FINISHED 状态
	err = switchToFinished(ctx, token, outTrackID, *content)
	if err != nil {
		fmt.Printf("完成状态失败：%v\n", err)
	}
	fmt.Println("✓ 卡片已完成")

	fmt.Println("\n✅ 测试完成！请在钉钉中查看。")
}

func getAccessToken(ctx context.Context, clientID, clientSecret string) (string, error) {
	body := map[string]string{"appKey": clientID, "appSecret": clientSecret}
	data, _ := json.Marshal(body)
	
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, tokenAPI, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	
	raw, _ := io.ReadAll(resp.Body)
	var result struct {
		AccessToken string `json:"accessToken"`
	}
	json.Unmarshal(raw, &result)
	return result.AccessToken, nil
}

func createCard(ctx context.Context, token, outTrackID string) error {
	body := map[string]interface{}{
		"cardTemplateId": aiCardTemplateID,
		"outTrackId":     outTrackID,
		"cardData": map[string]interface{}{
			"cardParamMap": map[string]string{
				"config": `{"autoLayout":true}`,
			},
		},
		"callbackType":          "STREAM",
		"imGroupOpenSpaceModel": map[string]bool{"supportForward": true},
		"imRobotOpenSpaceModel": map[string]bool{"supportForward": true},
	}
	data, _ := json.Marshal(body)
	
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, cardAPI, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	raw, _ := io.ReadAll(resp.Body)
	fmt.Printf("  Create: %s\n", string(raw))
	return nil
}

func deliverCard(ctx context.Context, token, outTrackID, userID, robotCode string) error {
	body := map[string]interface{}{
		"outTrackId":  outTrackID,
		"userId":      userID,
		"userIdType":  1,
		"openSpaceId": fmt.Sprintf("dtv1.card//IM_ROBOT.%s", userID),
		"imRobotOpenDeliverModel": map[string]string{
			"robotCode": robotCode,
		},
	}
	data, _ := json.Marshal(body)
	
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, deliverAPI, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	raw, _ := io.ReadAll(resp.Body)
	fmt.Printf("  Deliver: %s\n", string(raw))
	return nil
}

func switchToInputing(ctx context.Context, token, outTrackID string) error {
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"cardData": map[string]interface{}{
			"cardParamMap": map[string]string{
				"flowStatus":       "2",
				"msgContent":       "",
				"staticMsgContent": "",
				"sys_full_json_obj": `{"order":["msgContent"]}`,
			},
		},
	}
	data, _ := json.Marshal(body)
	
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, cardAPI, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	raw, _ := io.ReadAll(resp.Body)
	fmt.Printf("  Inputing: %s\n", string(raw))
	return nil
}

func streamUpdate(ctx context.Context, token, outTrackID, content string, finalize bool) error {
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"guid":       fmt.Sprintf("g%d", time.Now().UnixNano()),
		"key":        "msgContent",
		"content":    content,
		"isFull":     true,
		"isFinalize": finalize,
	}
	data, _ := json.Marshal(body)
	
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, streamingAPI, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	raw, _ := io.ReadAll(resp.Body)
	fmt.Printf("  Stream: %s\n", string(raw))
	return nil
}

func switchToFinished(ctx context.Context, token, outTrackID, content string) error {
	body := map[string]interface{}{
		"outTrackId": outTrackID,
		"cardData": map[string]interface{}{
			"cardParamMap": map[string]string{
				"flowStatus":       "3",
				"msgContent":       content,
				"staticMsgContent": content,
				"sys_full_json_obj": `{"order":["msgContent"]}`,
			},
			"cardUpdateOptions": map[string]bool{
				"updateCardDataByKey": true,
			},
		},
	}
	data, _ := json.Marshal(body)
	
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, cardAPI, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	raw, _ := io.ReadAll(resp.Body)
	fmt.Printf("  Finish: %s\n", string(raw))
	return nil
}
