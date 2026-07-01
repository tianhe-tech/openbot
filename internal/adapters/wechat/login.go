package wechat

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mdp/qrterminal/v3"
)

// LoginQR performs an interactive QR code login.
// It prints the QR code to stdout, polls for confirmation, and returns credentials.
func (c *Client) LoginQR() (*Credentials, error) {
	resp, err := c.GetBotQRCode(nil)
	if err != nil {
		return nil, fmt.Errorf("获取二维码失败: %w", err)
	}
	if resp.QRCodeImgContent == "" {
		return nil, fmt.Errorf("二维码内容为空")
	}

	qrterminal.GenerateHalfBlock(resp.QRCodeImgContent, qrterminal.L, os.Stdout)
	fmt.Printf("\n请使用微信扫描二维码登录\n备用链接: %s\n", resp.QRCodeImgContent)

	return c.pollQRLogin(resp.QRCode)
}

func (c *Client) pollQRLogin(qrcode string) (*Credentials, error) {
	timeout := 5 * time.Minute
	start := time.Now()
	var verifyCode string

	for time.Since(start) < timeout {
		statusResp, err := c.PollQRCodeStatus(qrcode, verifyCode)
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				continue
			}
			fmt.Printf("轮询状态出错: %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}

		switch statusResp.Status {
		case "confirmed":
			fmt.Printf("\n✅ 登录成功! Bot: %s\n", statusResp.IlinkBotID)
			return &Credentials{
				AccountID:   statusResp.IlinkBotID,
				BotToken:    statusResp.BotToken,
				IlinkBotID:  statusResp.IlinkBotID,
				BaseURL:     statusResp.BaseURL,
				NickName:    statusResp.NickName,
				AvatarURL:   statusResp.AvatarURL,
				IlinkUserID: statusResp.IlinkUserID,
			}, nil

		case "wait":
			fmt.Print(".")

		case "scaned":
			fmt.Println("\n✅ 已扫码，请在手机上确认")

		case "expired":
			return nil, fmt.Errorf("二维码已过期，请重新登录")

		case "need_verifycode":
			fmt.Print("\n请输入手机上显示的数字: ")
			fmt.Scanln(&verifyCode)

		case "verify_code_blocked":
			return nil, fmt.Errorf("验证码输入错误过多")

		case "scaned_but_redirect":
			fmt.Println("\n正在重定向到新节点...")

		case "binded_redirect":
			return nil, fmt.Errorf("此微信已绑定过本程序")

		default:
			fmt.Printf("\n未知状态: %s\n", statusResp.Status)
		}

		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("登录超时")
}
