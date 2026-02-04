package main

import (
	"context"
	"fmt"
	"os"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

func main() {
	appID := os.Getenv("FEISHU_APP_ID")
	appSecret := os.Getenv("FEISHU_APP_SECRET")

	if appID == "" || appSecret == "" {
		fmt.Println("请设置环境变量 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
		fmt.Println("例如:")
		fmt.Println("export FEISHU_APP_ID=cli_xxxx")
		fmt.Println("export FEISHU_APP_SECRET=xxxxx")
		os.Exit(1)
	}

	eventHandler := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(func(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
			fmt.Printf("[ OnP2MessageReceiveV1 access ], data: %s\n", larkcore.Prettify(event))
			return nil
		}).
		OnCustomizedEvent("message", func(ctx context.Context, event *larkevent.EventReq) error {
			fmt.Printf("[ OnCustomizedEvent access ], type: message, data: %s\n", string(event.Body))
			return nil
		})

	cli := larkws.NewClient(appID, appSecret,
		larkws.WithEventHandler(eventHandler),
		larkws.WithLogLevel(larkcore.LogLevelDebug),
	)

	fmt.Println("正在建立飞书长连接...")
	fmt.Printf("APP_ID: %s\n", appID)
	fmt.Println("连接成功后，向机器人发送消息即可接收测试")
	fmt.Println("按 Ctrl+C 退出程序")
	fmt.Println("================================================\n")

	err := cli.Start(context.Background())
	if err != nil {
		fmt.Printf("连接失败: %v\n", err)
		os.Exit(1)
	}
}
