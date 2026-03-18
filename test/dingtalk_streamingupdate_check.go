//go:build dingtalk_streamingupdate_check

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
	tokenAPI           = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
	createDeliverAPI   = "https://api.dingtalk.com/v1.0/card/instances/createAndDeliver"
	streamingUpdateAPI = "https://api.dingtalk.com/v1.0/card/streaming"
)

type accessTokenResponse struct {
	AccessToken string `json:"accessToken"`
	ExpireIn    int    `json:"expireIn"`
}

type streamingUpdateRequest struct {
	OutTrackID string `json:"outTrackId"`
	Guid       string `json:"guid"`
	Key        string `json:"key"`
	Content    string `json:"content"`
	IsFull     *bool  `json:"isFull,omitempty"`
	IsFinalize *bool  `json:"isFinalize,omitempty"`
	IsError    *bool  `json:"isError,omitempty"`
}

type streamingUpdateResponse struct {
	Success bool `json:"success"`
	Result  bool `json:"result"`
}

type createAndDeliverRequest struct {
	UserID            string                       `json:"userId,omitempty"`
	CardTemplateID    string                       `json:"cardTemplateId"`
	OutTrackID        string                       `json:"outTrackId"`
	CallbackType      string                       `json:"callbackType,omitempty"`
	CardData          createAndDeliverCardData     `json:"cardData"`
	OpenSpaceID       string                       `json:"openSpaceId"`
	OpenDeliverModels map[string]map[string]string `json:"openDeliverModels"`
	UserIDType        int                          `json:"userIdType,omitempty"`
}

type createAndDeliverCardData struct {
	CardParamMap map[string]string `json:"cardParamMap"`
}

type createAndDeliverResponse struct {
	Success bool `json:"success"`
	Result  struct {
		OutTrackID string `json:"outTrackId"`
	} `json:"result"`
}

func main() {
	clientID := strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_ID"))
	clientSecret := strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_SECRET"))

	autoCreate := flag.Bool("create", false, "create and deliver card before streaming update")
	cardTemplateID := flag.String("cardTemplateId", strings.TrimSpace(os.Getenv("DINGTALK_CARD_TEMPLATE_ID")), "card template id for createAndDeliver")
	userID := flag.String("userId", strings.TrimSpace(os.Getenv("DINGTALK_CARD_USER_ID")), "userId for IM_ROBOT deliver")
	spaceType := flag.String("spaceType", getEnvOrDefault("DINGTALK_SPACE_TYPE", "IM_ROBOT"), "space type: IM_ROBOT|IM_GROUP|IM_SINGLE|ONE_BOX")
	spaceID := flag.String("spaceId", strings.TrimSpace(os.Getenv("DINGTALK_SPACE_ID")), "space id, defaults to userId in IM_ROBOT")
	openSpaceID := flag.String("openSpaceId", strings.TrimSpace(os.Getenv("DINGTALK_OPEN_SPACE_ID")), "full openSpaceId like dtv1.card//IM_ROBOT.xxx")
	callbackType := flag.String("callbackType", getEnvOrDefault("DINGTALK_CALLBACK_TYPE", "STREAM"), "callback type: STREAM|HTTP")

	outTrackID := flag.String("outTrackId", strings.TrimSpace(os.Getenv("DINGTALK_OUT_TRACK_ID")), "AI card outTrackId")
	streamKey := flag.String("key", strings.TrimSpace(os.Getenv("DINGTALK_STREAM_KEY")), "AI stream variable key")
	content := flag.String("content", "hello from streaming update", "stream content")
	guid := flag.String("guid", fmt.Sprintf("stream-%d", time.Now().UnixNano()), "idempotent guid")
	isFull := flag.Bool("isFull", false, "set isFull=true, markdown content usually requires true")
	isFinalize := flag.Bool("finalize", true, "set isFinalize=true to finish streaming")
	isError := flag.Bool("error", false, "set isError=true to mark failed status")
	flag.Parse()

	if clientID == "" || clientSecret == "" {
		fmt.Println("缺少环境变量: DINGTALK_CLIENT_ID / DINGTALK_CLIENT_SECRET")
		fmt.Println("PowerShell 示例:")
		fmt.Println("  $env:DINGTALK_CLIENT_ID=\"你的ClientID\"")
		fmt.Println("  $env:DINGTALK_CLIENT_SECRET=\"你的ClientSecret\"")
		os.Exit(1)
	}
	if strings.TrimSpace(*streamKey) == "" {
		fmt.Println("缺少 AI 卡片参数: key")
		fmt.Println("可通过环境变量或命令行传入:")
		fmt.Println("  $env:DINGTALK_STREAM_KEY=\"ai_text\"")
		fmt.Println("或")
		fmt.Println("  go run -tags dingtalk_streamingupdate_check ./test/dingtalk_streamingupdate_check.go -key ai_text")
		os.Exit(1)
	}

	if !*autoCreate && strings.TrimSpace(*outTrackID) == "" {
		fmt.Println("缺少 AI 卡片参数: outTrackId / key")
		fmt.Println("可通过环境变量或命令行传入:")
		fmt.Println("  $env:DINGTALK_OUT_TRACK_ID=\"xxx\"")
		fmt.Println("  $env:DINGTALK_STREAM_KEY=\"ai_text\"")
		fmt.Println("或启用自动创建: -create -cardTemplateId <id> -userId <id>")
		fmt.Println("或")
		fmt.Println("  go run -tags dingtalk_streamingupdate_check ./test/dingtalk_streamingupdate_check.go -outTrackId xxx -key ai_text")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	token, err := getAccessToken(ctx, clientID, clientSecret)
	if err != nil {
		fmt.Printf("获取 accessToken 失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("获取 accessToken 成功")

	if *autoCreate {
		if strings.TrimSpace(*cardTemplateID) == "" {
			fmt.Println("启用 -create 时必须提供 cardTemplateId（或环境变量 DINGTALK_CARD_TEMPLATE_ID）")
			os.Exit(1)
		}
		if strings.TrimSpace(*outTrackID) == "" {
			*outTrackID = fmt.Sprintf("track-%d", time.Now().UnixNano())
		}

		resolvedSpaceID := strings.TrimSpace(*spaceID)
		if resolvedSpaceID == "" && strings.EqualFold(strings.TrimSpace(*spaceType), "IM_ROBOT") {
			resolvedSpaceID = strings.TrimSpace(*userID)
		}

		resolvedOpenSpaceID := strings.TrimSpace(*openSpaceID)
		if resolvedOpenSpaceID == "" {
			if resolvedSpaceID == "" {
				fmt.Println("无法生成 openSpaceId：请提供 -spaceId 或 -openSpaceId（IM_ROBOT 默认可用 -userId）")
				os.Exit(1)
			}
			resolvedOpenSpaceID = fmt.Sprintf("dtv1.card//%s.%s", strings.TrimSpace(*spaceType), resolvedSpaceID)
		}

		deliverKey := deliverModelKey(strings.TrimSpace(*spaceType))
		createReq := createAndDeliverRequest{
			UserID:         strings.TrimSpace(*userID),
			CardTemplateID: strings.TrimSpace(*cardTemplateID),
			OutTrackID:     strings.TrimSpace(*outTrackID),
			CallbackType:   strings.ToUpper(strings.TrimSpace(*callbackType)),
			CardData: createAndDeliverCardData{
				CardParamMap: map[string]string{
					strings.TrimSpace(*streamKey): *content,
				},
			},
			OpenSpaceID: resolvedOpenSpaceID,
			OpenDeliverModels: map[string]map[string]string{
				deliverKey: {},
			},
			UserIDType: 1,
		}

		createResp, createRaw, createErr := callCreateAndDeliver(ctx, token, createReq)
		if createErr != nil {
			fmt.Printf("调用 createAndDeliver 失败: %v\n", createErr)
			os.Exit(1)
		}
		fmt.Println("createAndDeliver 调用成功")
		fmt.Printf("create.success=%v outTrackId=%s\n", createResp.Success, createResp.Result.OutTrackID)
		if len(createRaw) > 0 {
			fmt.Printf("create.raw=%s\n", string(createRaw))
		}
	}

	req := streamingUpdateRequest{
		OutTrackID: strings.TrimSpace(*outTrackID),
		Guid:       strings.TrimSpace(*guid),
		Key:        strings.TrimSpace(*streamKey),
		Content:    *content,
	}
	if *isFull {
		v := true
		req.IsFull = &v
	}
	if *isFinalize {
		v := true
		req.IsFinalize = &v
	}
	if *isError {
		v := true
		req.IsError = &v
	}

	resp, raw, err := callStreamingUpdate(ctx, token, req)
	if err != nil {
		fmt.Printf("调用 streamingUpdate 失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("streamingUpdate 调用成功")
	fmt.Printf("success=%v result=%v\n", resp.Success, resp.Result)
	if len(raw) > 0 {
		fmt.Printf("raw=%s\n", string(raw))
	}
}

func getEnvOrDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func deliverModelKey(spaceType string) string {
	switch strings.ToUpper(strings.TrimSpace(spaceType)) {
	case "IM_GROUP":
		return "imGroupOpenDeliverModel"
	case "IM_SINGLE":
		return "imSingleOpenDeliverModel"
	case "ONE_BOX":
		return "topOpenDeliverModel"
	default:
		return "imRobotOpenDeliverModel"
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

func callStreamingUpdate(ctx context.Context, token string, payload streamingUpdateRequest) (streamingUpdateResponse, []byte, error) {
	var out streamingUpdateResponse

	data, err := json.Marshal(payload)
	if err != nil {
		return out, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, streamingUpdateAPI, bytes.NewReader(data))
	if err != nil {
		return out, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	hc := &http.Client{Timeout: 20 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return out, nil, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return out, raw, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
	}

	if err := json.Unmarshal(raw, &out); err != nil {
		return out, raw, fmt.Errorf("解析响应失败: %w, body=%s", err, string(raw))
	}
	return out, raw, nil
}

func callCreateAndDeliver(ctx context.Context, token string, payload createAndDeliverRequest) (createAndDeliverResponse, []byte, error) {
	var out createAndDeliverResponse

	data, err := json.Marshal(payload)
	if err != nil {
		return out, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, createDeliverAPI, bytes.NewReader(data))
	if err != nil {
		return out, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	hc := &http.Client{Timeout: 20 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return out, nil, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return out, raw, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
	}

	if err := json.Unmarshal(raw, &out); err != nil {
		return out, raw, fmt.Errorf("解析响应失败: %w, body=%s", err, string(raw))
	}
	return out, raw, nil
}
