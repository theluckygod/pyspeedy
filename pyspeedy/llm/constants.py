from string import Template

system_avatar = "https://img.freepik.com/premium-vector/vr-headset-icon-vector-image-can-be-used-gaming-ecommerce_120816-193062.jpg?w=826"
bot_avatar = "https://img.freepik.com/free-vector/chatbot-chat-message-vectorart_78370-4104.jpg?t=st=1712839858~exp=1712843458~hmac=961c1c722b37437bc6c6f1516f87acd7ce214350d732924040b10fa2f9cf4875&w=826"
user_avatar = "https://img.freepik.com/premium-vector/icon-man-s-face-with-light-skin_238404-1006.jpg?w=826"


format_chat_html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Chat Interface</title>
<style>
    body {
        // background: #ffffff;
        margin-top: 10px;
    }
    .chat-box {
        height: 100%;
        width: 100%;
        background-color: #fff;
        overflow: hidden;
    }
    .chats {
        padding: 30px 15px;
    }
    .chat-avatar {
        float: right;
    }
    .chat-avatar .avatar {
        width: 30px;
        box-shadow: 0 2px 2px 0 rgba(0,0,0,0.2), 0 6px 10px 0 rgba(0,0,0,0.3);
    }
    .chat-body {
        display: block;
        margin: 10px 30px 0 0;
        overflow: hidden;
    }
    .chat-body:first-child {
        margin-top: 0;
    }
    .chat-content {
        position: relative;
        display: block;
        float: right;
        padding: 8px 15px;
        margin: 0 20px 10px 0;
        clear: both;
        color: #fff;
        background-color: #62a8ea;
        border-radius: 4px;
        box-shadow: 0 1px 4px 0 rgba(0,0,0,0.37);
    }
    .chat-content:before {
        position: absolute;
        top: 10px;
        right: -10px;
        width: 0;
        height: 0;
        content: '';
        border: 5px solid transparent;
        border-left-color: #62a8ea;
    }
    .chat-content>p:last-child {
        margin-bottom: 0;
    }
    .chat-left .chat-avatar {
        float: left;
    }
    .chat-left .chat-body {
        margin-right: 0;
        margin-left: 30px;
    }
    .chat-left .chat-content {
        float: left;
        margin: 0 0 10px 20px;
        color: #76838f;
        background-color: #dfe9ef;
    }
    .chat-left .chat-content:before {
        right: auto;
        left: -10px;
        border-right-color: #dfe9ef;
        border-left-color: transparent;
    }
    .panel-footer {
        padding: 0 30px 15px;
        background-color: transparent;
        border-top: 1px solid transparent;
        border-bottom-right-radius: 3px;
        border-bottom-left-radius: 3px;
    }
    .avatar img {
        width: 100%;
        max-width: 100%;
        height: auto;
        border: 0 none;
        border-radius: 1000px;
    }
    .chat-avatar .avatar {
        width: 30px;
    }
    .avatar {
        position: relative;
        display: inline-block;
        width: 40px;
        white-space: nowrap;
        border-radius: 1000px;
        vertical-align: bottom;
    }
</style>
</head>
<body>
<div class="container bootstrap snippets bootdeys">
<div class="col-md-7 col-xs-12 col-md-offset-2">
  <!-- Panel Chat -->
  <div class="panel" id="chat">
    <div class="panel-heading">
      <h3 class="panel-title">
        <i class="icon wb-chat-text" aria-hidden="true"></i> Chat
      </h3>
    </div>
    <div class="panel-body">
      <div class="chats">
        $chats
      </div>
    </div>
  </div>
  <!-- End Panel Chat -->
</div>
</div>
</body>
</html>
"""


format_chat_html_template = Template(format_chat_html)

bot_chat_html = """
<div class="chat chat-left">
    <div class="chat-avatar">
        <a class="avatar avatar-online" data-toggle="tooltip" href="#" data-placement="left" title="{title}">
            <img src="{avatar}" alt="...">
            <i></i>
        </a>
    </div>
    <div class="chat-body">
        <div class="chat-content">
            <p>{message}</p>
        </div>
    </div>
</div>"""

user_chat_html = """
<div class="chat">
    <div class="chat-avatar">
        <a class="avatar avatar-online" data-toggle="tooltip" href="#" data-placement="right" title="{title}">
            <img src="{avatar}" alt="...">
            <i></i>
        </a>
    </div>
    <div class="chat-body">
        <div class="chat-content">
            <p>{message}</p>
        </div>
    </div>
</div>"""
