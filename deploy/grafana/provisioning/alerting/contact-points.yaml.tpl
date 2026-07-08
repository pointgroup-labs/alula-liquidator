apiVersion: 1

contactPoints:
  - orgId: 1
    name: telegram
    receivers:
      - uid: telegram-primary
        type: telegram
        settings:
          bottoken: $TELEGRAM_BOT_TOKEN
          chatid: "$TELEGRAM_CHAT_ID"
          parse_mode: HTML
        disableResolveMessage: false
