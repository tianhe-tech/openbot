// cmd/wechat-login is a CLI tool for managing WeChat bot credentials.
//
// Usage:
//
//	go run ./cmd/wechat-login --login          # Scan QR to login
//	go run ./cmd/wechat-login --list           # List saved accounts
//	go run ./cmd/wechat-login --delete <id>    # Delete a saved account
//	go run ./cmd/wechat-login --show <id>      # Show env vars for an account
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/user/opencode-gateway/internal/adapters/wechat"
)

func main() {
	stateDir := flag.String("state-dir", "", "credential storage directory (default ~/.opencode-gateway-wechat)")
	loginCmd := flag.Bool("login", false, "扫码登录微信")
	listCmd := flag.Bool("list", false, "列出已保存的账号")
	deleteCmd := flag.String("delete", "", "删除指定账号")
	showCmd := flag.String("show", "", "显示指定账号的环境变量配置")
	flag.Parse()

	dir := *stateDir
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".opencode-gateway-wechat")
	}

	store := wechat.NewStore(dir)
	if err := store.Init(); err != nil {
		log.Fatalf("初始化存储失败: %v", err)
	}

	client := wechat.NewClient()

	switch {
	case *loginCmd:
		handleLogin(client, store)
	case *listCmd:
		handleList(store)
	case *deleteCmd != "":
		handleDelete(store, *deleteCmd)
	case *showCmd != "":
		handleShow(store, *showCmd)
	default:
		flag.Usage()
		fmt.Println("\n示例:")
		fmt.Println("  go run ./cmd/wechat-login --login       # 扫码登录")
		fmt.Println("  go run ./cmd/wechat-login --list         # 列出账号")
		fmt.Println("  go run ./cmd/wechat-login --show <id>    # 显示环境变量")
	}
}

func handleLogin(client *wechat.Client, store *wechat.Store) {
	cred, err := client.LoginQR()
	if err != nil {
		log.Fatalf("登录失败: %v", err)
	}

	if err := store.SaveCredentials(cred); err != nil {
		log.Fatalf("保存凭证失败: %v", err)
	}

	fmt.Printf("\n✅ 登录成功并已保存!\n")
	fmt.Printf("  账号ID:   %s\n", cred.AccountID)
	fmt.Printf("  昵称:     %s\n", cred.NickName)
	fmt.Printf("  后端地址: %s\n", cred.BaseURL)
	fmt.Println("\n📋 请将以下环境变量添加到 .env 或启动配置中:")
	fmt.Printf("  WECHAT_BOT_TOKEN=%s\n", cred.BotToken)
	fmt.Printf("  WECHAT_BASE_URL=%s\n", cred.BaseURL)
	fmt.Printf("  WECHAT_ACCOUNT_ID=%s\n", cred.AccountID)
}

func handleList(store *wechat.Store) {
	accounts, err := store.ListAccounts()
	if err != nil {
		log.Fatalf("列出账号失败: %v", err)
	}
	if len(accounts) == 0 {
		fmt.Println("没有保存的账号，请先运行 --login")
		return
	}
	fmt.Println("已保存的账号:")
	for _, acc := range accounts {
		cred, err := store.LoadCredentials(acc)
		if err != nil {
			fmt.Printf("  %s (加载失败: %v)\n", acc, err)
			continue
		}
		nick := cred.NickName
		if nick == "" {
			nick = "(未命名)"
		}
		fmt.Printf("  %s - %s (URL: %s)\n", acc, nick, cred.BaseURL)
	}
}

func handleDelete(store *wechat.Store, accountID string) {
	accounts, err := store.ListAccounts()
	if err != nil {
		log.Fatalf("列出账号失败: %v", err)
	}
	found := false
	for _, a := range accounts {
		if a == accountID {
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("账号 %s 不存在", accountID)
	}

	// Delete by removing the file
	home, _ := os.UserHomeDir()
	dir := filepath.Join(home, ".opencode-gateway-wechat", "accounts")
	path := filepath.Join(dir, accountID+".json")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Fatalf("删除失败: %v", err)
	}
	fmt.Printf("已删除账号: %s\n", accountID)
}

func handleShow(store *wechat.Store, accountID string) {
	cred, err := store.LoadCredentials(accountID)
	if err != nil {
		log.Fatalf("加载账号失败: %v", err)
	}
	fmt.Printf("# 微信账号: %s (%s)\n", cred.AccountID, cred.NickName)
	fmt.Printf("WECHAT_BOT_TOKEN=%s\n", cred.BotToken)
	fmt.Printf("WECHAT_BASE_URL=%s\n", cred.BaseURL)
	fmt.Printf("WECHAT_ACCOUNT_ID=%s\n", cred.AccountID)
}
