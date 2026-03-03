package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	defaultDingTalkUID = "054349580632603835"
	tokenAPI           = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
)

type accessTokenResponse struct {
	AccessToken string `json:"accessToken"`
	ExpireIn    int    `json:"expireIn"`
}

func main() {
	clientID := os.Getenv("DINGTALK_CLIENT_ID")
	clientSecret := os.Getenv("DINGTALK_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		fmt.Println("缺少环境变量: DINGTALK_CLIENT_ID / DINGTALK_CLIENT_SECRET")
		fmt.Println("PowerShell 示例:")
		fmt.Println("  $env:DINGTALK_CLIENT_ID=\"你的ClientID\"")
		fmt.Println("  $env:DINGTALK_CLIENT_SECRET=\"你的ClientSecret\"")
		os.Exit(1)
	}

	uid := defaultDingTalkUID
	if len(os.Args) > 1 && os.Args[1] != "" {
		uid = os.Args[1]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("=== DingTalk 用户详情查询 ===")
	fmt.Printf("UID: %s\n", uid)

	token, err := getAccessToken(ctx, clientID, clientSecret)
	if err != nil {
		fmt.Printf("获取 access_token 失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("获取 access_token 成功")

	if err := queryUserV1(ctx, token, uid); err != nil {
		fmt.Printf("v1.0 contact/users 查询失败: %v\n", err)
	} else {
		return
	}

	fmt.Println("尝试旧版接口 topapi/v2/user/get ...")
	if err := queryUserLegacy(ctx, token, uid); err != nil {
		fmt.Printf("旧版接口也失败: %v\n", err)
		os.Exit(1)
	}
}

func getAccessToken(ctx context.Context, clientID, clientSecret string) (string, error) {
	bodyMap := map[string]string{
		"appKey":    clientID,
		"appSecret": clientSecret,
	}
	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenAPI, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	hc := &http.Client{Timeout: 15 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
	}

	var tokenResp accessTokenResponse
	if err := json.Unmarshal(raw, &tokenResp); err != nil {
		return "", fmt.Errorf("解析 token 响应失败: %w, body=%s", err, string(raw))
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("token 为空, body=%s", string(raw))
	}

	return tokenResp.AccessToken, nil
}

func queryUserV1(ctx context.Context, token, uid string) error {
	url := fmt.Sprintf("https://api.dingtalk.com/v1.0/contact/users/%s?language=zh_CN", uid)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-acs-dingtalk-access-token", token)

	hc := &http.Client{Timeout: 15 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
	}

	fmt.Println("v1.0 contact/users 查询成功，返回如下:")
	fmt.Println(prettyJSON(raw))
	return nil
}

func queryUserLegacy(ctx context.Context, token, uid string) error {
	url := "https://oapi.dingtalk.com/topapi/v2/user/get?access_token=" + token
	bodyMap := map[string]string{"userid": uid}
	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	hc := &http.Client{Timeout: 15 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
	}

	fmt.Println("topapi/v2/user/get 查询成功，返回如下:")
	fmt.Println(prettyJSON(raw))
	return nil
}

func prettyJSON(raw []byte) string {
	var obj interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return string(raw)
	}
	b, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(b)
}
