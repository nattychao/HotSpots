# coding=utf-8
"""
消息发送器模块

将报告数据发送到各种通知渠道：
- 飞书 (Feishu/Lark)
- 钉钉 (DingTalk)
- 企业微信 (WeCom/WeWork)
- Telegram
- 邮件 (Email)
- ntfy
- Bark
- Slack
- 极光推送 JPush（iOS App）

每个发送函数都支持分批发送，并通过参数化配置实现与 CONFIG 的解耦。
"""

import base64
import hashlib
import hmac
import smtplib
import time
import json
from datetime import datetime
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import quote_plus, urlparse

import requests

from trendradar.utils.time import DEFAULT_TIMEZONE, get_configured_time

from .batch import add_batch_headers, get_max_batch_header_size
from .formatters import convert_markdown_to_mrkdwn, strip_markdown


def _extract_ai_stats(ai_analysis) -> Optional[Dict]:
    """从 AI 分析结果中提取统计数据"""
    if not ai_analysis or not getattr(ai_analysis, "success", False):
        return None
    return {
        "total_news": getattr(ai_analysis, "total_news", 0),
        "analyzed_news": getattr(ai_analysis, "analyzed_news", 0),
        "max_news_limit": getattr(ai_analysis, "max_news_limit", 0),
        "hotlist_count": getattr(ai_analysis, "hotlist_count", 0),
        "rss_count": getattr(ai_analysis, "rss_count", 0),
        "hotlist_analyzed": getattr(ai_analysis, "hotlist_analyzed", 0),
        "rss_analyzed": getattr(ai_analysis, "rss_analyzed", 0),
        "standalone_analyzed": getattr(ai_analysis, "standalone_analyzed", 0),
        "ai_mode": getattr(ai_analysis, "ai_mode", ""),
        "include_rss": getattr(ai_analysis, "include_rss", True),
        "include_standalone": getattr(ai_analysis, "include_standalone", False),
    }


def _render_ai_analysis(ai_analysis: Any, channel: str) -> str:
    """渲染 AI 分析内容为指定渠道格式"""
    if not ai_analysis:
        return ""

    try:
        from trendradar.ai.formatter import get_ai_analysis_renderer
        renderer = get_ai_analysis_renderer(channel)
        return renderer(ai_analysis)
    except ImportError:
        return ""


# === SMTP 邮件配置 ===
SMTP_CONFIGS = {
    # Gmail（使用 STARTTLS）
    "gmail.com": {"server": "smtp.gmail.com", "port": 587, "encryption": "TLS"},
    # QQ邮箱（使用 SSL，更稳定）
    "qq.com": {"server": "smtp.qq.com", "port": 465, "encryption": "SSL"},
    # Outlook（使用 STARTTLS）
    "outlook.com": {"server": "smtp-mail.outlook.com", "port": 587, "encryption": "TLS"},
    "hotmail.com": {"server": "smtp-mail.outlook.com", "port": 587, "encryption": "TLS"},
    "live.com": {"server": "smtp-mail.outlook.com", "port": 587, "encryption": "TLS"},
    # 网易邮箱（使用 SSL，更稳定）
    "163.com": {"server": "smtp.163.com", "port": 465, "encryption": "SSL"},
    "126.com": {"server": "smtp.126.com", "port": 465, "encryption": "SSL"},
    # 新浪邮箱（使用 SSL）
    "sina.com": {"server": "smtp.sina.com", "port": 465, "encryption": "SSL"},
    # 搜狐邮箱（使用 SSL）
    "sohu.com": {"server": "smtp.sohu.com", "port": 465, "encryption": "SSL"},
    # 天翼邮箱（使用 SSL）
    "189.cn": {"server": "smtp.189.cn", "port": 465, "encryption": "SSL"},
    # 阿里云邮箱（使用 TLS）
    "aliyun.com": {"server": "smtp.aliyun.com", "port": 465, "encryption": "TLS"},
    # Yandex邮箱（使用 TLS）
    "yandex.com": {"server": "smtp.yandex.com", "port": 465, "encryption": "TLS"},
    # iCloud邮箱（使用 SSL）
    "icloud.com": {"server": "smtp.mail.me.com", "port": 587, "encryption": "SSL"},
}


def send_to_feishu(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 29000,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    get_time_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到飞书（支持分批发送，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: 飞书 Webhook URL
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        get_time_func: 获取当前时间的函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # 日志前缀
    log_prefix = f"飞书{account_label}" if account_label else "飞书"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "feishu") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 预留批次头部空间，避免添加头部后超限
    header_reserve = get_max_batch_header_size("feishu")
    batches = split_content_func(
        report_data,
        "feishu",
        update_info,
        max_bytes=batch_size - header_reserve,
        mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "feishu", batch_size)

    print(f"{log_prefix}消息分为 {len(batches)} 批次发送 [{report_type}]")

    # 逐批发送
    for i, batch_content in enumerate(batches, 1):
        content_size = len(batch_content.encode("utf-8"))
        print(
            f"发送{log_prefix}第 {i}/{len(batches)} 批次，大小：{content_size} 字节 [{report_type}]"
        )

        # 根据 webhook 域名选择 payload 格式
        # www.feishu.cn 使用纯文本格式，其他域名（open.feishu.cn/open.larksuite.com）使用卡片 2.0
        if "www.feishu.cn" in webhook_url:
            payload = {
                "msg_type": "text",
                "content": {
                    "text": batch_content,
                },
            }
        else:
            payload = {
                "msg_type": "interactive",
                "card": {
                    "schema": "2.0",
                    "body": {
                        "elements": [
                            {"tag": "markdown", "content": batch_content}
                        ]
                    },
                },
            }

        try:
            response = requests.post(
                webhook_url, headers=headers, json=payload, proxies=proxies, timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                # 检查飞书的响应状态
                if result.get("StatusCode") == 0 or result.get("code") == 0:
                    print(f"{log_prefix}第 {i}/{len(batches)} 批次发送成功 [{report_type}]")
                    # 批次间间隔
                    if i < len(batches):
                        time.sleep(batch_interval)
                else:
                    error_msg = result.get("msg") or result.get("StatusMessage", "未知错误")
                    print(
                        f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，错误：{error_msg}"
                    )
                    return False
            else:
                print(
                    f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，状态码：{response.status_code}"
                )
                return False
        except Exception as e:
            print(f"{log_prefix}第 {i}/{len(batches)} 批次发送出错 [{report_type}]：{e}")
            return False

    print(f"{log_prefix}所有 {len(batches)} 批次发送完成 [{report_type}]")

    return True


def sign_dingtalk_url(webhook_url: str, secret: str) -> str:
    """给钉钉机器人 webhook 加上 timestamp 和 sign（加签模式）。

    钉钉的加签算法：把 `${timestamp}\\n${secret}` 用 secret 做 HmacSHA256，
    再 base64、再 URL 编码。timestamp 是毫秒，且必须在一小时内，所以**每次
    发送都要重新算**，不能预先算好存起来。

    机器人安全设置里勾了"加签"就必须带这两个参数，否则钉钉返回
    `errcode 310000, sign not match`。没开加签的机器人带上也无害——参数多于
    要求时钉钉只校验要求的那部分，不过为了避免困惑，只在配置了 secret 时才加。
    """
    if not secret:
        return webhook_url

    timestamp = str(round(time.time() * 1000))
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(
        secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    sign = quote_plus(base64.b64encode(digest))

    separator = "&" if "?" in webhook_url else "?"
    return f"{webhook_url}{separator}timestamp={timestamp}&sign={sign}"


def send_to_dingtalk(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    secret: str = "",
    batch_size: int = 20000,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到钉钉（支持分批发送，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: 钉钉 Webhook URL
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # 日志前缀
    log_prefix = f"钉钉{account_label}" if account_label else "钉钉"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "dingtalk") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 预留批次头部空间，避免添加头部后超限
    header_reserve = get_max_batch_header_size("dingtalk")
    batches = split_content_func(
        report_data,
        "dingtalk",
        update_info,
        max_bytes=batch_size - header_reserve,
        mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "dingtalk", batch_size)

    print(f"{log_prefix}消息分为 {len(batches)} 批次发送 [{report_type}]")

    # 逐批发送
    for i, batch_content in enumerate(batches, 1):
        content_size = len(batch_content.encode("utf-8"))
        print(
            f"发送{log_prefix}第 {i}/{len(batches)} 批次，大小：{content_size} 字节 [{report_type}]"
        )

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"TrendRadar 热点分析报告 - {report_type}",
                "text": batch_content,
            },
        }

        try:
            # 每批重新签名：timestamp 必须在一小时内，批次间可能间隔很久。
            response = requests.post(
                sign_dingtalk_url(webhook_url, secret),
                headers=headers,
                json=payload,
                proxies=proxies,
                timeout=30,
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("errcode") == 0:
                    print(f"{log_prefix}第 {i}/{len(batches)} 批次发送成功 [{report_type}]")
                    # 批次间间隔
                    if i < len(batches):
                        time.sleep(batch_interval)
                else:
                    print(
                        f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，错误：{result.get('errmsg')}"
                    )
                    return False
            else:
                print(
                    f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，状态码：{response.status_code}"
                )
                return False
        except Exception as e:
            print(f"{log_prefix}第 {i}/{len(batches)} 批次发送出错 [{report_type}]：{e}")
            return False

    print(f"{log_prefix}所有 {len(batches)} 批次发送完成 [{report_type}]")

    return True


def send_to_wework(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 4000,
    batch_interval: float = 1.0,
    msg_type: str = "markdown",
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到企业微信（支持分批发送，支持 markdown 和 text 两种格式，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: 企业微信 Webhook URL
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        msg_type: 消息类型 (markdown/text)
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # 日志前缀
    log_prefix = f"企业微信{account_label}" if account_label else "企业微信"

    # 获取消息类型配置（markdown 或 text）
    is_text_mode = msg_type.lower() == "text"

    if is_text_mode:
        print(f"{log_prefix}使用 text 格式（个人微信模式）[{report_type}]")
    else:
        print(f"{log_prefix}使用 markdown 格式（群机器人模式）[{report_type}]")

    # text 模式使用 wework_text，markdown 模式使用 wework
    header_format_type = "wework_text" if is_text_mode else "wework"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "wework") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size(header_format_type)
    batches = split_content_func(
        report_data, "wework", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, header_format_type, batch_size)

    print(f"{log_prefix}消息分为 {len(batches)} 批次发送 [{report_type}]")

    # 逐批发送
    for i, batch_content in enumerate(batches, 1):
        # 根据消息类型构建 payload
        if is_text_mode:
            # text 格式：去除 markdown 语法
            plain_content = strip_markdown(batch_content)
            payload = {"msgtype": "text", "text": {"content": plain_content}}
            content_size = len(plain_content.encode("utf-8"))
        else:
            # markdown 格式：保持原样
            payload = {"msgtype": "markdown", "markdown": {"content": batch_content}}
            content_size = len(batch_content.encode("utf-8"))

        print(
            f"发送{log_prefix}第 {i}/{len(batches)} 批次，大小：{content_size} 字节 [{report_type}]"
        )

        try:
            response = requests.post(
                webhook_url, headers=headers, json=payload, proxies=proxies, timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("errcode") == 0:
                    print(f"{log_prefix}第 {i}/{len(batches)} 批次发送成功 [{report_type}]")
                    # 批次间间隔
                    if i < len(batches):
                        time.sleep(batch_interval)
                else:
                    print(
                        f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，错误：{result.get('errmsg')}"
                    )
                    return False
            else:
                print(
                    f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，状态码：{response.status_code}"
                )
                return False
        except Exception as e:
            print(f"{log_prefix}第 {i}/{len(batches)} 批次发送出错 [{report_type}]：{e}")
            return False

    print(f"{log_prefix}所有 {len(batches)} 批次发送完成 [{report_type}]")

    return True


def send_to_telegram(
    bot_token: str,
    chat_id: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 4000,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到 Telegram（支持分批发送，支持热榜+RSS合并+独立展示区）

    Args:
        bot_token: Telegram Bot Token
        chat_id: Telegram Chat ID
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # 日志前缀
    log_prefix = f"Telegram{account_label}" if account_label else "Telegram"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "telegram") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size("telegram")
    batches = split_content_func(
        report_data, "telegram", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "telegram", batch_size)

    print(f"{log_prefix}消息分为 {len(batches)} 批次发送 [{report_type}]")

    # 逐批发送
    for i, batch_content in enumerate(batches, 1):
        content_size = len(batch_content.encode("utf-8"))
        print(
            f"发送{log_prefix}第 {i}/{len(batches)} 批次，大小：{content_size} 字节 [{report_type}]"
        )

        payload = {
            "chat_id": chat_id,
            "text": batch_content,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            response = requests.post(
                url, headers=headers, json=payload, proxies=proxies, timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("ok"):
                    print(f"{log_prefix}第 {i}/{len(batches)} 批次发送成功 [{report_type}]")
                    # 批次间间隔
                    if i < len(batches):
                        time.sleep(batch_interval)
                else:
                    print(
                        f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，错误：{result.get('description')}"
                    )
                    return False
            else:
                print(
                    f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，状态码：{response.status_code}"
                )
                return False
        except Exception as e:
            print(f"{log_prefix}第 {i}/{len(batches)} 批次发送出错 [{report_type}]：{e}")
            return False

    print(f"{log_prefix}所有 {len(batches)} 批次发送完成 [{report_type}]")

    return True


def send_to_email(
    from_email: str,
    password: str,
    to_email: str,
    report_type: str,
    html_file_path: str,
    custom_smtp_server: Optional[str] = None,
    custom_smtp_port: Optional[int] = None,
    *,
    get_time_func: Callable = None,
) -> bool:
    """
    发送邮件通知

    Args:
        from_email: 发件人邮箱
        password: 邮箱密码/授权码
        to_email: 收件人邮箱（多个用逗号分隔）
        report_type: 报告类型
        html_file_path: HTML 报告文件路径
        custom_smtp_server: 自定义 SMTP 服务器（可选）
        custom_smtp_port: 自定义 SMTP 端口（可选）
        get_time_func: 获取当前时间的函数

    Returns:
        bool: 发送是否成功

    Note:
        AI 分析内容已在 HTML 生成时嵌入，无需再追加
    """
    try:
        if not html_file_path or not Path(html_file_path).exists():
            print(f"错误：HTML文件不存在或未提供: {html_file_path}")
            return False

        print(f"使用HTML文件: {html_file_path}")
        with open(html_file_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        domain = from_email.split("@")[-1].lower()

        if custom_smtp_server and custom_smtp_port:
            # 使用自定义 SMTP 配置
            smtp_server = custom_smtp_server
            smtp_port = int(custom_smtp_port)
            # 根据端口判断加密方式：465=SSL, 587=TLS
            if smtp_port == 465:
                use_tls = False  # SSL 模式（SMTP_SSL）
            elif smtp_port == 587:
                use_tls = True  # TLS 模式（STARTTLS）
            else:
                # 其他端口优先尝试 TLS（更安全，更广泛支持）
                use_tls = True
        elif domain in SMTP_CONFIGS:
            # 使用预设配置
            config = SMTP_CONFIGS[domain]
            smtp_server = config["server"]
            smtp_port = config["port"]
            use_tls = config["encryption"] == "TLS"
        else:
            print(f"未识别的邮箱服务商: {domain}，使用通用 SMTP 配置")
            smtp_server = f"smtp.{domain}"
            smtp_port = 587
            use_tls = True

        msg = MIMEMultipart("alternative")

        # 严格按照 RFC 标准设置 From header
        sender_name = "TrendRadar"
        msg["From"] = formataddr((sender_name, from_email))

        # 设置收件人
        recipients = [addr.strip() for addr in to_email.split(",")]
        if len(recipients) == 1:
            msg["To"] = recipients[0]
        else:
            msg["To"] = ", ".join(recipients)

        # 设置邮件主题
        now = get_time_func() if get_time_func else datetime.now()
        subject = f"TrendRadar 热点分析报告 - {report_type} - {now.strftime('%m月%d日 %H:%M')}"
        msg["Subject"] = Header(subject, "utf-8")

        # 设置其他标准 header
        msg["MIME-Version"] = "1.0"
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid()

        # 添加纯文本部分（作为备选）
        text_content = f"""
TrendRadar 热点分析报告
========================
报告类型：{report_type}
生成时间：{now.strftime('%Y-%m-%d %H:%M:%S')}

请使用支持HTML的邮件客户端查看完整报告内容。
        """
        text_part = MIMEText(text_content, "plain", "utf-8")
        msg.attach(text_part)

        html_part = MIMEText(html_content, "html", "utf-8")
        msg.attach(html_part)

        print(f"正在发送邮件到 {to_email}...")
        print(f"SMTP 服务器: {smtp_server}:{smtp_port}")
        print(f"发件人: {from_email}")

        try:
            if use_tls:
                # TLS 模式
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.set_debuglevel(0)  # 设为1可以查看详细调试信息
                server.ehlo()
                server.starttls()
                server.ehlo()
            else:
                # SSL 模式
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)
                server.set_debuglevel(0)
                server.ehlo()

            # 登录
            server.login(from_email, password)

            # 发送邮件
            server.send_message(msg)
            server.quit()

            print(f"邮件发送成功 [{report_type}] -> {to_email}")
            return True

        except smtplib.SMTPServerDisconnected:
            print("邮件发送失败：服务器意外断开连接，请检查网络或稍后重试")
            return False

    except smtplib.SMTPAuthenticationError as e:
        print("邮件发送失败：认证错误，请检查邮箱和密码/授权码")
        print(f"详细错误: {str(e)}")
        return False
    except smtplib.SMTPRecipientsRefused as e:
        print(f"邮件发送失败：收件人地址被拒绝 {e}")
        return False
    except smtplib.SMTPSenderRefused as e:
        print(f"邮件发送失败：发件人地址被拒绝 {e}")
        return False
    except smtplib.SMTPDataError as e:
        print(f"邮件发送失败：邮件数据错误 {e}")
        return False
    except smtplib.SMTPConnectError as e:
        print(f"邮件发送失败：无法连接到 SMTP 服务器 {smtp_server}:{smtp_port}")
        print(f"详细错误: {str(e)}")
        return False
    except Exception as e:
        print(f"邮件发送失败 [{report_type}]：{e}")
        import traceback
        traceback.print_exc()
        return False


def send_to_ntfy(
    server_url: str,
    topic: str,
    token: Optional[str],
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 3800,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到 ntfy（支持分批发送，严格遵守4KB限制，支持热榜+RSS合并+独立展示区）

    Args:
        server_url: ntfy 服务器 URL
        topic: ntfy 主题
        token: ntfy 访问令牌（可选）
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    # 日志前缀
    log_prefix = f"ntfy{account_label}" if account_label else "ntfy"

    # 避免 HTTP header 编码问题
    report_type_en_map = {
        "全天汇总": "Daily Summary",
        "当前榜单": "Current Ranking",
        "增量分析": "Incremental Update",
        "通知连通性测试": "Notification Test",
    }
    report_type_en = report_type_en_map.get(report_type, "News Report")

    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Markdown": "yes",
        "Title": report_type_en,
        "Priority": "default",
        "Tags": "news",
    }

    if token:
        headers["Authorization"] = f"Bearer {token}"

    # 构建完整URL，确保格式正确
    base_url = server_url.rstrip("/")
    if not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"
    url = f"{base_url}/{topic}"

    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "ntfy") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size("ntfy")
    batches = split_content_func(
        report_data, "ntfy", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "ntfy", batch_size)

    total_batches = len(batches)
    print(f"{log_prefix}消息分为 {total_batches} 批次发送 [{report_type}]")

    # 反转批次顺序，使得在ntfy客户端显示时顺序正确
    # ntfy显示最新消息在上面，所以我们从最后一批开始推送
    reversed_batches = list(reversed(batches))

    print(f"{log_prefix}将按反向顺序推送（最后批次先推送），确保客户端显示顺序正确")

    # 逐批发送（反向顺序）
    success_count = 0
    for idx, batch_content in enumerate(reversed_batches, 1):
        # 计算正确的批次编号（用户视角的编号）
        actual_batch_num = total_batches - idx + 1

        content_size = len(batch_content.encode("utf-8"))
        print(
            f"发送{log_prefix}第 {actual_batch_num}/{total_batches} 批次（推送顺序: {idx}/{total_batches}），大小：{content_size} 字节 [{report_type}]"
        )

        # 检查消息大小，确保不超过4KB
        if content_size > 4096:
            print(f"警告：{log_prefix}第 {actual_batch_num} 批次消息过大（{content_size} 字节），可能被拒绝")

        # 更新 headers 的批次标识
        current_headers = headers.copy()
        if total_batches > 1:
            current_headers["Title"] = f"{report_type_en} ({actual_batch_num}/{total_batches})"

        try:
            response = requests.post(
                url,
                headers=current_headers,
                data=batch_content.encode("utf-8"),
                proxies=proxies,
                timeout=30,
            )

            if response.status_code == 200:
                print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次发送成功 [{report_type}]")
                success_count += 1
                if idx < total_batches:
                    # 公共服务器建议 2-3 秒，自托管可以更短
                    interval = 2 if "ntfy.sh" in server_url else 1
                    time.sleep(interval)
            elif response.status_code == 429:
                print(
                    f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次速率限制 [{report_type}]，等待后重试"
                )
                time.sleep(10)  # 等待10秒后重试
                # 重试一次
                retry_response = requests.post(
                    url,
                    headers=current_headers,
                    data=batch_content.encode("utf-8"),
                    proxies=proxies,
                    timeout=30,
                )
                if retry_response.status_code == 200:
                    print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次重试成功 [{report_type}]")
                    success_count += 1
                else:
                    print(
                        f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次重试失败，状态码：{retry_response.status_code}"
                    )
            elif response.status_code == 413:
                print(
                    f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次消息过大被拒绝 [{report_type}]，消息大小：{content_size} 字节"
                )
            else:
                print(
                    f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次发送失败 [{report_type}]，状态码：{response.status_code}"
                )
                try:
                    print(f"错误详情：{response.text}")
                except:
                    pass

        except requests.exceptions.ConnectTimeout:
            print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次连接超时 [{report_type}]")
        except requests.exceptions.ReadTimeout:
            print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次读取超时 [{report_type}]")
        except requests.exceptions.ConnectionError as e:
            print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次连接错误 [{report_type}]：{e}")
        except Exception as e:
            print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次发送异常 [{report_type}]：{e}")

    # 判断整体发送是否成功
    if success_count == total_batches:
        print(f"{log_prefix}所有 {total_batches} 批次发送完成 [{report_type}]")
    elif success_count > 0:
        print(f"{log_prefix}部分发送成功：{success_count}/{total_batches} 批次 [{report_type}]")
    else:
        print(f"{log_prefix}发送完全失败 [{report_type}]")
        return False

    return True


def send_to_bark(
    bark_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 3600,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到 Bark（支持分批发送，使用 markdown 格式，支持热榜+RSS合并+独立展示区）

    Args:
        bark_url: Bark URL（包含 device_key）
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    # 日志前缀
    log_prefix = f"Bark{account_label}" if account_label else "Bark"

    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # 解析 Bark URL，提取 device_key 和 API 端点
    # Bark URL 格式: https://api.day.app/device_key 或 https://bark.day.app/device_key
    parsed_url = urlparse(bark_url)
    device_key = parsed_url.path.strip('/').split('/')[0] if parsed_url.path else None

    if not device_key:
        print(f"{log_prefix} URL 格式错误，无法提取 device_key: {bark_url}")
        return False

    # 构建正确的 API 端点
    api_endpoint = f"{parsed_url.scheme}://{parsed_url.netloc}/push"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "bark") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size("bark")
    batches = split_content_func(
        report_data, "bark", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "bark", batch_size)

    total_batches = len(batches)
    print(f"{log_prefix}消息分为 {total_batches} 批次发送 [{report_type}]")

    # 反转批次顺序，使得在Bark客户端显示时顺序正确
    # Bark显示最新消息在上面，所以我们从最后一批开始推送
    reversed_batches = list(reversed(batches))

    print(f"{log_prefix}将按反向顺序推送（最后批次先推送），确保客户端显示顺序正确")

    # 逐批发送（反向顺序）
    success_count = 0
    for idx, batch_content in enumerate(reversed_batches, 1):
        # 计算正确的批次编号（用户视角的编号）
        actual_batch_num = total_batches - idx + 1

        content_size = len(batch_content.encode("utf-8"))
        print(
            f"发送{log_prefix}第 {actual_batch_num}/{total_batches} 批次（推送顺序: {idx}/{total_batches}），大小：{content_size} 字节 [{report_type}]"
        )

        # 检查消息大小（Bark使用APNs，限制4KB）
        if content_size > 4096:
            print(
                f"警告：{log_prefix}第 {actual_batch_num}/{total_batches} 批次消息过大（{content_size} 字节），可能被拒绝"
            )

        # 构建JSON payload
        payload = {
            "title": report_type,
            "markdown": batch_content,
            "device_key": device_key,
            "sound": "default",
            "group": "TrendRadar",
            "action": "none",  # 点击推送跳到 APP 不弹出弹框,方便阅读
        }

        try:
            response = requests.post(
                api_endpoint,
                json=payload,
                proxies=proxies,
                timeout=30,
            )

            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 200:
                    print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次发送成功 [{report_type}]")
                    success_count += 1
                    # 批次间间隔
                    if idx < total_batches:
                        time.sleep(batch_interval)
                else:
                    print(
                        f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次发送失败 [{report_type}]，错误：{result.get('message', '未知错误')}"
                    )
            else:
                print(
                    f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次发送失败 [{report_type}]，状态码：{response.status_code}"
                )
                try:
                    print(f"错误详情：{response.text}")
                except:
                    pass

        except requests.exceptions.ConnectTimeout:
            print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次连接超时 [{report_type}]")
        except requests.exceptions.ReadTimeout:
            print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次读取超时 [{report_type}]")
        except requests.exceptions.ConnectionError as e:
            print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次连接错误 [{report_type}]：{e}")
        except Exception as e:
            print(f"{log_prefix}第 {actual_batch_num}/{total_batches} 批次发送异常 [{report_type}]：{e}")

    # 判断整体发送是否成功
    if success_count == total_batches:
        print(f"{log_prefix}所有 {total_batches} 批次发送完成 [{report_type}]")
    elif success_count > 0:
        print(f"{log_prefix}部分发送成功：{success_count}/{total_batches} 批次 [{report_type}]")
    else:
        print(f"{log_prefix}发送完全失败 [{report_type}]")
        return False

    return True


def send_to_slack(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 4000,
    batch_interval: float = 1.0,
    split_content_func: Callable = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到 Slack（支持分批发送，使用 mrkdwn 格式，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: Slack Webhook URL
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    headers = {"Content-Type": "application/json"}
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # 日志前缀
    log_prefix = f"Slack{account_label}" if account_label else "Slack"

    # 渲染 AI 分析内容并提取统计数据
    ai_content = _render_ai_analysis(ai_analysis, "slack") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容，预留批次头部空间
    header_reserve = get_max_batch_header_size("slack")
    batches = split_content_func(
        report_data, "slack", update_info, max_bytes=batch_size - header_reserve, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部（已预留空间，不会超限）
    batches = add_batch_headers(batches, "slack", batch_size)

    print(f"{log_prefix}消息分为 {len(batches)} 批次发送 [{report_type}]")

    # 逐批发送
    for i, batch_content in enumerate(batches, 1):
        # 转换 Markdown 到 mrkdwn 格式
        mrkdwn_content = convert_markdown_to_mrkdwn(batch_content)

        content_size = len(mrkdwn_content.encode("utf-8"))
        print(
            f"发送{log_prefix}第 {i}/{len(batches)} 批次，大小：{content_size} 字节 [{report_type}]"
        )

        # 构建 Slack payload（使用简单的 text 字段，支持 mrkdwn）
        payload = {"text": mrkdwn_content}

        try:
            response = requests.post(
                webhook_url, headers=headers, json=payload, proxies=proxies, timeout=30
            )

            # Slack Incoming Webhooks 成功时返回 "ok" 文本
            if response.status_code == 200 and response.text == "ok":
                print(f"{log_prefix}第 {i}/{len(batches)} 批次发送成功 [{report_type}]")
                # 批次间间隔
                if i < len(batches):
                    time.sleep(batch_interval)
            else:
                error_msg = response.text if response.text else f"状态码：{response.status_code}"
                print(
                    f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，错误：{error_msg}"
                )
                return False
        except Exception as e:
            print(f"{log_prefix}第 {i}/{len(batches)} 批次发送出错 [{report_type}]：{e}")
            return False

    print(f"{log_prefix}所有 {len(batches)} 批次发送完成 [{report_type}]")

    return True


def send_to_generic_webhook(
    webhook_url: str,
    payload_template: Optional[str],
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    account_label: str = "",
    *,
    batch_size: int = 4000,
    batch_interval: float = 1.0,
    split_content_func: Optional[Callable] = None,
    rss_items: Optional[list] = None,
    rss_new_items: Optional[list] = None,
    ai_analysis: Any = None,
    display_regions: Optional[Dict] = None,
    standalone_data: Optional[Dict] = None,
) -> bool:
    """
    发送到通用 Webhook（支持分批发送，支持自定义 JSON 模板，支持热榜+RSS合并+独立展示区）

    Args:
        webhook_url: Webhook URL
        payload_template: JSON 模板字符串，支持 {title} 和 {content} 占位符
        report_data: 报告数据
        report_type: 报告类型
        update_info: 更新信息（可选）
        proxy_url: 代理 URL（可选）
        mode: 报告模式 (daily/current)
        account_label: 账号标签（多账号时显示）
        batch_size: 批次大小（字节）
        batch_interval: 批次发送间隔（秒）
        split_content_func: 内容分批函数
        rss_items: RSS 统计条目列表（可选，用于合并推送）
        rss_new_items: RSS 新增条目列表（可选，用于新增区块）

    Returns:
        bool: 发送是否成功
    """
    if split_content_func is None:
        raise ValueError("split_content_func is required")

    headers = {"Content-Type": "application/json"}
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # 日志前缀
    log_prefix = f"通用Webhook{account_label}" if account_label else "通用Webhook"

    # 渲染 AI 分析内容并提取统计数据（通用 Webhook 使用 markdown 格式）
    ai_content = _render_ai_analysis(ai_analysis, "wework") if ai_analysis else None
    ai_stats = _extract_ai_stats(ai_analysis)

    # 获取分批内容
    # 使用 'wework' 作为 format_type 以获取 markdown 格式的通用输出
    # 预留一定空间给模板外壳
    template_overhead = 200
    batches = split_content_func(
        report_data, "wework", update_info, max_bytes=batch_size - template_overhead, mode=mode,
        rss_items=rss_items,
        rss_new_items=rss_new_items,
        ai_content=ai_content,
        standalone_data=standalone_data,
        ai_stats=ai_stats,
        report_type=report_type,
    )

    # 统一添加批次头部
    batches = add_batch_headers(batches, "wework", batch_size)

    print(f"{log_prefix}消息分为 {len(batches)} 批次发送 [{report_type}]")

    # 逐批发送
    for i, batch_content in enumerate(batches, 1):
        content_size = len(batch_content.encode("utf-8"))
        print(
            f"发送{log_prefix}第 {i}/{len(batches)} 批次，大小：{content_size} 字节 [{report_type}]"
        )

        try:
            # 构建 payload
            if payload_template:
                # 简单的字符串替换
                # 注意：content 可能包含 JSON 特殊字符，需要先转义
                json_content = json.dumps(batch_content)[1:-1] # 去掉首尾引号
                json_title = json.dumps(report_type)[1:-1]
                
                payload_str = payload_template.replace("{content}", json_content).replace("{title}", json_title)
                
                # 尝试解析为 JSON 对象以验证有效性
                try:
                    payload = json.loads(payload_str)
                except json.JSONDecodeError as e:
                    print(f"{log_prefix} JSON 模板解析失败: {e}")
                    # 回退到默认格式
                    payload = {"title": report_type, "content": batch_content}
            else:
                # 默认格式
                payload = {"title": report_type, "content": batch_content}

            response = requests.post(
                webhook_url, headers=headers, json=payload, proxies=proxies, timeout=30
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                print(f"{log_prefix}第 {i}/{len(batches)} 批次发送成功 [{report_type}]")
                if i < len(batches):
                    time.sleep(batch_interval)
            else:
                print(
                    f"{log_prefix}第 {i}/{len(batches)} 批次发送失败 [{report_type}]，状态码：{response.status_code}, 响应: {response.text}"
                )
                return False
        except Exception as e:
            print(f"{log_prefix}第 {i}/{len(batches)} 批次发送出错 [{report_type}]：{e}")
            return False

    print(f"{log_prefix}所有 {len(batches)} 批次发送完成 [{report_type}]")

    return True


# ===============================================================
# 极光推送 JPush（经 APNs 推送到 iOS App）
# ===============================================================

JPUSH_API_URL = "https://api.jpush.cn/v3/push"
# 极光要求 notification.ios 整体不超过 3584 字节（UTF-8，一个汉字 3 字节），
# 它再加上自己的字段交给 APNs（上限 4096）。
JPUSH_IOS_LIMIT = 3584
# 通知正文里列出的条数，其余在附带数据里。
JPUSH_BODY_ITEMS = 3


def _rank_text(ranks) -> str:
    """排名的可读形式："7" 或 "7-8"。没有排名时给空串。"""
    if not ranks:
        return ""
    unique = sorted({int(r) for r in ranks if isinstance(r, (int, float))})
    if not unique:
        return ""
    low, high = unique[0], unique[-1]
    return str(low) if low == high else f"{low}-{high}"


def _hotspot_level(ranks, rank_threshold) -> str:
    """排名的强调级别：前 3 算 top，进入阈值算 hot，其余 normal。

    网页报告用红色/橙色徽标区分前几名；推送沿用同一套语义，让两处看起来
    是同一个产品，而不是两套风格。
    """
    if not ranks:
        return "normal"
    numeric = [int(r) for r in ranks if isinstance(r, (int, float))]
    if not numeric:
        return "normal"
    low = min(numeric)
    if low <= 3:
        return "top"
    if low <= rank_threshold:
        return "hot"
    return "normal"


def _collect_hotspot_items(report_data: Dict, standalone_data: Optional[Dict] = None) -> list:
    """
    按报告顺序取出热点条目（去重），每条：标题、来源、链接、分组、是否新增，
    以及网页报告里同样会显示的排名、时间跨度、出现次数。

    先是关键词匹配到的（分组为关键词组），再是独立展示区各平台的榜单
    （分组为平台名，例如“今日头条”）。

    字段用短名，因为载荷只有 3.5KB，长名会把条目挤掉：
      t 标题 / s 来源 / u 链接 / k 分组 / n 新增
      r 排名 "7-8" / lv 级别 top|hot|normal / tm 时间跨度 / c 出现次数
    """
    items = []
    seen = set()

    def add(title, source, url, group, is_new, ranks=None, rank_threshold=10, time_display="", count=0):
        title = (title or "").strip()
        if not title:
            return
        key = (title, url)
        if key in seen:
            return
        seen.add(key)
        item = {"t": title, "s": source, "u": url, "k": group, "n": bool(is_new)}
        rank = _rank_text(ranks)
        if rank:
            item["r"] = rank
            item["lv"] = _hotspot_level(ranks, rank_threshold)
        if time_display:
            item["tm"] = time_display
        if count:
            item["c"] = int(count)
        items.append(item)

    for stat in report_data.get("stats", []) or []:
        word = stat.get("word", "")
        for title_data in stat.get("titles", []) or []:
            add(
                title_data.get("title"),
                title_data.get("source_name", ""),
                # 热榜条目由 analyzer 写入的是驼峰 mobileUrl；RSS 条目是
                # snake_case mobile_url。两种都读，否则手机上打开的是 PC 页。
                title_data.get("mobileUrl")
                or title_data.get("mobile_url")
                or title_data.get("url")
                or "",
                word,
                title_data.get("is_new", False),
                ranks=title_data.get("ranks") or [],
                rank_threshold=title_data.get("rank_threshold", 10),
                time_display=title_data.get("time_display", ""),
                count=title_data.get("count", 0),
            )

    for platform in (standalone_data or {}).get("platforms", []) or []:
        name = platform.get("name") or platform.get("id", "")
        for item in platform.get("items", []) or []:
            add(
                item.get("title"),
                name,
                item.get("mobileUrl") or item.get("url") or "",
                name,
                False,
                ranks=item.get("ranks") or ([item["rank"]] if item.get("rank") else []),
                rank_threshold=item.get("rank_threshold", 10),
                time_display=item.get("time_display", ""),
                count=item.get("count", 0),
            )
    return items


def _ios_size(ios: Dict) -> int:
    return len(json.dumps(ios, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def build_jpush_payload(
    report_data: Dict,
    report_type: str,
    tag: Optional[str],
    production: bool,
    mode: str = "daily",
    limit: int = JPUSH_IOS_LIMIT,
    standalone_data: Optional[Dict] = None,
    timezone: str = DEFAULT_TIMEZONE,
) -> Optional[Dict]:
    """
    组装发给极光的请求体，没有热点时返回 None。

    一次报告一条推送。tag 留空（默认）是广播：发给所有注册了的设备——App 只在用户
    打开热点推送后才注册极光，关掉时解绑，所以就是开了热点推送的设备。广播免费账号
    每天最多 10 次。填了 tag 只发给打了这个标签的设备，但按标签推送是极光的 VIP 功能，
    免费账号会被拒（错误码 2009）。

    - 通知标题：热点 · 报告类型
    - 通知正文：前几条标题，多出的写“等 N 条”
    - 附带数据 extras.hotspot：条目列表（t 标题 / s 来源 / u 链接 / k 关键词组 / n 是否新增），
      按顺序放到 notification.ios 装得下为止，total 是本次报告的总条数
    - options.apns_production：推到 APNs 的生产环境（True，TestFlight / App Store 包）
      还是开发环境（False，Xcode 直接装的包），必须与 App 的签名一致
    """
    items = _collect_hotspot_items(report_data, standalone_data)
    if not items:
        return None

    lines = [f"• {item['t']}" for item in items[:JPUSH_BODY_ITEMS]]
    if len(items) > JPUSH_BODY_ITEMS:
        lines.append(f"等 {len(items)} 条")

    # 头部统计：和网页报告的紫色表头同样的四项中的三项。新闻总数指这次抓取
    # 到的全部条目（含没匹配到关键词的），与 hotlist_total 一致；没有这个
    # 数据时退回条目数。
    #
    # 时间用配置时区的当前时间，不是 `datetime.now()`：GitHub Actions 的
    # 容器是 UTC，直接取会和手机上看到的差八小时。项目里其它地方都用
    # `get_configured_time`，这里也照做。
    news_total = report_data.get("hotlist_total") or len(items)
    header = {
        "report": report_type,
        "mode": mode,
        "total": len(items),
        "news": news_total,
        "generated": get_configured_time(timezone).strftime("%m-%d %H:%M"),
        "items": [],
    }
    ios = {
        "alert": {"title": f"热点 · {report_type}", "body": "\n".join(lines)},
        "sound": "default",
        "thread-id": "trendradar",
        "extras": {"hotspot": header},
    }

    carried = header["items"]
    for item in items:
        carried.append(item)
        if _ios_size(ios) > limit:
            carried.pop()
            break

    # 正文本身就超了（极长的标题）：截短正文
    while _ios_size(ios) > limit and ios["alert"]["body"]:
        ios["alert"]["body"] = ios["alert"]["body"][:-20]

    return {
        "platform": ["ios"],
        "audience": {"tag": [tag]} if tag else "all",
        "notification": {"ios": ios},
        # 一天内没上线的设备，上线后补发；之后不再补
        "options": {"apns_production": bool(production), "time_to_live": 86400},
    }


def describe_jpush_payload(payload: Dict) -> str:
    """推送内容的可读版本，打印到日志里：通知标题、正文和附带的每一条，与手机收到的一致。"""
    ios = payload.get("notification", {}).get("ios", {})
    alert = ios.get("alert", {})
    hotspot = ios.get("extras", {}).get("hotspot", {})
    items = hotspot.get("items", [])
    production = payload.get("options", {}).get("apns_production")
    audience = payload.get("audience")
    target = "广播（所有设备）" if audience == "all" else "标签 " + ", ".join(audience.get("tag", []))
    lines = [
        "──── 极光推送内容 ────",
        f"发给：{target}    "
        f"环境：{'生产' if production else '开发'}    大小：{_ios_size(ios)} 字节",
        f"标题：{alert.get('title', '')}",
        "正文：",
    ]
    lines += [f"  {line}" for line in (alert.get("body") or "").split("\n")]
    lines.append(f"附带条目：{len(items)}/{hotspot.get('total', len(items))}")
    for index, item in enumerate(items, 1):
        new = " 🆕" if item.get("n") else ""
        lines.append(f"  {index}. [{item.get('k', '')}] {item.get('t', '')}（{item.get('s', '')}）{new}")
        if item.get("u"):
            lines.append(f"     {item['u']}")
    lines.append("──────────────────────────────")
    return "\n".join(lines)


def parse_jpush_production(value) -> bool:
    """JPUSH_APNS_PRODUCTION 的值：true/1/yes/on 为生产环境，其余（含留空）为开发环境。"""
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("true", "1", "yes", "on")


def send_to_jpush(
    app_key: str,
    master_secret: str,
    tag: Optional[str],
    production: bool,
    report_data: Dict,
    report_type: str,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    standalone_data: Optional[Dict] = None,
    timezone: str = DEFAULT_TIMEZONE,
) -> bool:
    """
    发送到极光推送：广播（tag 留空）或推给打了 tag 的设备，极光交给 APNs，送到 iOS App。

    Args:
        app_key: 极光应用的 AppKey
        master_secret: 极光应用的 Master Secret（只放在 Secrets 里）
        tag: 标签（VIP 功能）；留空则广播
        production: True 推 APNs 生产环境，False 推开发环境
        report_data: 报告数据
        report_type: 报告类型
        proxy_url: 代理 URL（可选）
        mode: 报告模式
        standalone_data: 独立展示区数据（可选，各平台榜单前几条）
        timezone: 报告时间的时区，默认 Asia/Shanghai

    Returns:
        bool: 发送是否成功（没有热点可推时也返回 True）
    """
    payload = build_jpush_payload(
        report_data, report_type, tag, production, mode,
        standalone_data=standalone_data, timezone=timezone,
    )
    if payload is None:
        print(f"极光推送 没有可推送的热点，跳过 [{report_type}]")
        return True

    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    hotspot = payload["notification"]["ios"]["extras"]["hotspot"]
    target = f"标签 {tag}" if tag else "广播"
    print(f"发送 极光推送 {target}（{'生产' if production else '开发'}环境），附带 {len(hotspot['items'])}/{hotspot['total']} 条 [{report_type}]")
    print(describe_jpush_payload(payload))

    try:
        response = requests.post(
            JPUSH_API_URL,
            json=payload,
            auth=(app_key, master_secret),
            proxies=proxies,
            timeout=30,
        )
    except requests.exceptions.RequestException as e:
        print(f"极光推送 发送异常 [{report_type}]：{e}")
        return False

    try:
        body = response.json()
    except ValueError:
        body = {}

    if response.status_code == 200:
        print(f"极光推送 发送成功 [{report_type}] msg_id={body.get('msg_id', '')}")
        return True

    error = body.get("error", {}) if isinstance(body, dict) else {}
    code, message = error.get("code"), error.get("message", "")
    hint = {
        1004: "AppKey 或 Master Secret 不对",
        1011: "没有可推的设备：App 里打开热点推送，并确认它的环境与 JPUSH_APNS_PRODUCTION 一致",
        2006: "这是极光的 VIP 功能，免费账号不能用",
        2008: "广播超过了每天 10 次的上限（免费账号），明天恢复",
        2009: "按标签推送是极光的 VIP 功能：把 JPUSH_TAG 留空改用广播",
    }.get(code, "")
    print(
        f"极光推送 发送失败 [{report_type}]，状态码：{response.status_code}，错误码：{code}，"
        f"详情：{message or response.text[:500]}" + (f"（{hint}）" if hint else "")
    )
    return False
