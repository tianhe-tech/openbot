package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/card"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/event"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/logger"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/payload"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/plugin"
)

/**
 * @Author linya.jj
 * @Date 2023/3/22 18:30
 */

// 简单的应答机器人实现
func OnChatBotMessageReceived(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	fmt.Printf("received chatbot callback, data=[%+v]\n", data)
	replyMsg := []byte(fmt.Sprintf("msg received: [%s]", data.Text.Content))

	chatbotReplier := chatbot.NewChatbotReplier()
	if err := chatbotReplier.SimpleReplyText(ctx, data.SessionWebhook, replyMsg); err != nil {
		return nil, err
	}
	if err := chatbotReplier.SimpleReplyMarkdown(ctx, data.SessionWebhook, []byte("Markdown消息"), replyMsg); err != nil {
		return nil, err
	}

	return []byte(""), nil
}

// 简单的插件处理实现
func OnPluginMessageReceived(ctx context.Context, request *plugin.GraphRequest) (*plugin.GraphResponse, error) {
	response := &plugin.GraphResponse{
		Body: `{"text": "hello world", "content": [{"title": "1", "description": "2", "url":"https://www.zhihu.com/question/626551401"},{"title": "2", "description": "2", "url":"https://www.zhihu.com/question/626551401"}]}`,
	}
	return response, nil
}

// 事件处理
func OnEventReceived(ctx context.Context, df *payload.DataFrame) (frameResp *payload.DataFrameResponse, err error) {
	eventHeader := event.NewEventHeaderFromDataFrame(df)

	logger.GetLogger().Infof("received event, eventId=[%s] eventBornTime=[%d] eventCorpId=[%s] eventType=[%s] eventUnifiedAppId=[%s] data=[%s]",
		eventHeader.EventId,
		eventHeader.EventBornTime,
		eventHeader.EventCorpId,
		eventHeader.EventType,
		eventHeader.EventUnifiedAppId,
		df.Data)

	frameResp = payload.NewSuccessDataFrameResponse()
	if err := frameResp.SetJson(event.NewEventProcessResultSuccess()); err != nil {
		return nil, err
	}

	return
}

func OnCardCallbackReceived(ctx context.Context, request *card.CardRequest) (*card.CardResponse, error) {
	logger.GetLogger().Infof("receive card data: %v", request)
	response := &card.CardResponse{
		CardData: &card.CardDataDto{},
	}
	return response, nil
}

// go run example/*.go --client_id your-client-id --client_secret your-client-secret
func main() {
	var clientId, clientSecret string
	flag.StringVar(&clientId, "client_id", "", "DingTalk client id")
	flag.StringVar(&clientSecret, "client_secret", "", "DingTalk client secret")

	flag.Parse()

	clientId = strings.TrimSpace(clientId)
	clientSecret = strings.TrimSpace(clientSecret)

	if clientId == "" {
		clientId = strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_ID"))
	}
	if clientSecret == "" {
		clientSecret = strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_SECRET"))
	}

	if clientId == "" || clientSecret == "" {
		fmt.Println("缺少凭证: 请通过参数或环境变量提供 client_id/client_secret")
		fmt.Println("示例1: go run .\\test\\dingtalk_stream_receive_check.go -client_id <id> -client_secret <secret>")
		fmt.Println("示例2(PS): $env:DINGTALK_CLIENT_ID=\"<id>\"; $env:DINGTALK_CLIENT_SECRET=\"<secret>\"; go run .\\test\\dingtalk_stream_receive_check.go")
		os.Exit(1)
	}

	logger.SetLogger(logger.NewStdTestLoggerWithDebug())

	cli := client.NewStreamClient(client.WithAppCredential(client.NewAppCredentialConfig(clientId, clientSecret)))

	//注册事件类型的处理函数
	cli.RegisterAllEventRouter(OnEventReceived)
	//注册callback类型的处理函数
	cli.RegisterChatBotCallbackRouter(OnChatBotMessageReceived)
	//注册插件的处理函数
	cli.RegisterPluginCallbackRouter(OnPluginMessageReceived)
	//注册互动卡片类型的处理函数
	cli.RegisterCardCallbackRouter(OnCardCallbackReceived)

	err := cli.Start(context.Background())
	if err != nil {
		fmt.Printf("启动 Stream 失败: %v\n", err)
		os.Exit(1)
	}

	defer cli.Close()

	select {}
}
