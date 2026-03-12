module github.com/user/opencode-gateway

go 1.24.3

require (
	github.com/aliyun/alibabacloud-nls-go-sdk v1.1.1
	github.com/gorilla/websocket v1.5.0
	github.com/larksuite/oapi-sdk-go/v3 v3.5.3
	github.com/open-dingtalk/dingtalk-stream-sdk-go v0.9.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/sst/opencode-sdk-go v0.19.2
)

replace github.com/open-dingtalk/dingtalk-stream-sdk-go => ./third_party/dingtalk-stream-sdk-go

require (
	github.com/aliyun/alibaba-cloud-sdk-go v1.61.1376 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	gopkg.in/ini.v1 v1.66.2 // indirect
)
