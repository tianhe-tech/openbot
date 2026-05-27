package wechat

// --- QR Login Types ---

type GetBotQRCodeRequest struct {
	LocalTokenList []string `json:"local_token_list"`
}

type GetBotQRCodeResponse struct {
	QRCode           string `json:"qrcode"`
	QRCodeImgContent string `json:"qrcode_img_content"`
}

type QRCodeStatusResponse struct {
	Status       string `json:"status"`
	BotToken     string `json:"bot_token,omitempty"`
	IlinkBotID   string `json:"ilink_bot_id,omitempty"`
	BaseURL      string `json:"baseurl,omitempty"`
	NickName     string `json:"nick_name,omitempty"`
	AvatarURL    string `json:"avatar_url,omitempty"`
	IlinkUserID  string `json:"ilink_user_id,omitempty"`
	RedirectHost string `json:"redirect_host,omitempty"`
}

// --- GetUpdates Types ---

type WeixinMessage struct {
	Seq          int           `json:"seq,omitempty"`
	MessageID    int64         `json:"message_id,omitempty"`
	FromUserID   string        `json:"from_user_id,omitempty"`
	ToUserID     string        `json:"to_user_id,omitempty"`
	ClientID     string        `json:"client_id,omitempty"`
	CreateTimeMs int64         `json:"create_time_ms,omitempty"`
	SessionID    string        `json:"session_id,omitempty"`
	GroupID      string        `json:"group_id,omitempty"`
	MessageType  int           `json:"message_type,omitempty"`
	MessageState int           `json:"message_state,omitempty"`
	ItemList     []MessageItem `json:"item_list,omitempty"`
	ContextToken string        `json:"context_token,omitempty"`
}

type MessageItem struct {
	Type         int         `json:"type,omitempty"`
	CreateTimeMs int64       `json:"create_time_ms,omitempty"`
	IsCompleted  bool        `json:"is_completed,omitempty"`
	MsgID        string      `json:"msg_id,omitempty"`
	TextItem     *TextItem   `json:"text_item,omitempty"`
	ImageItem    *ImageItem  `json:"image_item,omitempty"`
	VoiceItem    *VoiceItem  `json:"voice_item,omitempty"`
	FileItem     *FileItem   `json:"file_item,omitempty"`
	VideoItem    *VideoItem  `json:"video_item,omitempty"`
	RefMsg       *RefMessage `json:"ref_msg,omitempty"`
}

type TextItem struct {
	Text string `json:"text,omitempty"`
}

type CDNMedia struct {
	EncryptQueryParam string `json:"encrypt_query_param,omitempty"`
	AESKey            string `json:"aes_key,omitempty"`
	EncryptType       int    `json:"encrypt_type,omitempty"`
	FullURL           string `json:"full_url,omitempty"`
}

type ImageItem struct {
	Media      *CDNMedia `json:"media,omitempty"`
	ThumbMedia *CDNMedia `json:"thumb_media,omitempty"`
	AESKey     string    `json:"aeskey,omitempty"`
	URL        string    `json:"url,omitempty"`
	MidSize    int       `json:"mid_size,omitempty"`
	ThumbSize  int       `json:"thumb_size,omitempty"`
	ThumbW     int       `json:"thumb_width,omitempty"`
	ThumbH     int       `json:"thumb_height,omitempty"`
	HdSize     int       `json:"hd_size,omitempty"`
}

type VoiceItem struct {
	Media        *CDNMedia `json:"media,omitempty"`
	EncodeType   int       `json:"encode_type,omitempty"`
	BitsPerSampl int       `json:"bits_per_sample,omitempty"`
	SampleRate   int       `json:"sample_rate,omitempty"`
	Playtime     int       `json:"playtime,omitempty"`
	Text         string    `json:"text,omitempty"`
}

type FileItem struct {
	Media    *CDNMedia `json:"media,omitempty"`
	FileName string    `json:"file_name,omitempty"`
	MD5      string    `json:"md5,omitempty"`
	Len      string    `json:"len,omitempty"`
}

type VideoItem struct {
	Media      *CDNMedia `json:"media,omitempty"`
	VideoSize  int       `json:"video_size,omitempty"`
	PlayLength int       `json:"play_length,omitempty"`
	VideoMD5   string    `json:"video_md5,omitempty"`
	ThumbMedia *CDNMedia `json:"thumb_media,omitempty"`
	ThumbSize  int       `json:"thumb_size,omitempty"`
	ThumbH     int       `json:"thumb_height,omitempty"`
	ThumbW     int       `json:"thumb_width,omitempty"`
}

type RefMessage struct {
	MessageItem *MessageItem `json:"message_item,omitempty"`
	Title       string       `json:"title,omitempty"`
}

type GetUpdatesResp struct {
	Ret             int             `json:"ret"`
	Errcode         int             `json:"errcode,omitempty"`
	Errmsg          string          `json:"errmsg,omitempty"`
	Msgs            []WeixinMessage `json:"msgs"`
	GetUpdatesBuf   string          `json:"get_updates_buf,omitempty"`
	LongPollTimeout int             `json:"longpolling_timeout_ms,omitempty"`
}

// --- SendMessage Types ---

type SendMessageResp struct {
	Ret     int    `json:"ret,omitempty"`
	Errcode int    `json:"errcode,omitempty"`
	Errmsg  string `json:"errmsg,omitempty"`
}

// --- GetConfig Types ---

type GetConfigResp struct {
	Ret          int    `json:"ret,omitempty"`
	Errmsg       string `json:"errmsg,omitempty"`
	TypingTicket string `json:"typing_ticket,omitempty"`
}

// --- Common ---

type BaseInfo struct {
	ChannelVersion string `json:"channel_version,omitempty"`
	BotAgent       string `json:"bot_agent,omitempty"`
}

// Credentials stores WeChat bot login info.
type Credentials struct {
	AccountID   string `json:"account_id"`
	BotToken    string `json:"bot_token"`
	IlinkBotID  string `json:"ilink_bot_id"`
	BaseURL     string `json:"base_url"`
	NickName    string `json:"nick_name,omitempty"`
	AvatarURL   string `json:"avatar_url,omitempty"`
	IlinkUserID string `json:"ilink_user_id,omitempty"`
}

// --- Constants ---

const (
	MessageTypeNone = 0
	MessageTypeUser = 1
	MessageTypeBot  = 2

	ItemTypeNone  = 0
	ItemTypeText  = 1
	ItemTypeImage = 2
	ItemTypeVoice = 3
	ItemTypeFile  = 4
	ItemTypeVideo = 5

	TypingStatusTyping = 1
	TypingStatusCancel = 2

	MessageStateNew        = 0
	MessageStateGenerating = 1
	MessageStateFinish     = 2

	DefaultBotType = "3"
	FixedBaseURL   = "https://ilinkai.weixin.qq.com"

	sessionExpiredCode = -14
)
