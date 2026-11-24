"""JPush payload: one push per report, broadcast (or to a tag), within JPush's iOS size limit."""

import json
import unittest

from unittest import mock

from trendradar.notification.senders import (
    JPUSH_IOS_LIMIT,
    build_jpush_payload,
    describe_jpush_payload,
    parse_jpush_production,
    send_to_jpush,
)


def _report(count, title_length=10):
    titles = [
        {
            "title": f"热点{i}" + "字" * title_length,
            "source_name": "微博",
            "url": f"https://example.com/{i}",
            "mobile_url": f"https://m.example.com/{i}" if i % 2 else "",
            "is_new": i == 0,
        }
        for i in range(count)
    ]
    return {"stats": [{"word": "科技", "titles": titles}]}


def _ios_size(payload):
    return len(json.dumps(payload["notification"]["ios"], ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _hotspot(payload):
    return payload["notification"]["ios"]["extras"]["hotspot"]


def _build(report, report_type="当前榜单", production=False, tag=None, **kwargs):
    return build_jpush_payload(report, report_type, tag, production, **kwargs)


class JPushPayloadTests(unittest.TestCase):
    def test_nothing_to_push(self):
        self.assertIsNone(_build({"stats": []}))

    def test_broadcast_on_ios_in_the_chosen_environment(self):
        payload = _build(_report(1), production=False)
        self.assertEqual(payload["platform"], ["ios"])
        # 按标签推送是 VIP 功能（免费账号 2009），默认广播
        self.assertEqual(payload["audience"], "all")
        self.assertIs(payload["options"]["apns_production"], False)
        self.assertEqual(_build(_report(1), tag="hotspot")["audience"], {"tag": ["hotspot"]})
        self.assertEqual(_build(_report(1), tag="")["audience"], "all")
        self.assertIs(_build(_report(1), production=True)["options"]["apns_production"], True)

    def test_alert_and_items(self):
        payload = _build(_report(5), mode="current")
        ios = payload["notification"]["ios"]
        self.assertEqual(ios["thread-id"], "trendradar")
        alert = ios["alert"]
        self.assertEqual(alert["title"], "热点 · 当前榜单")
        self.assertEqual(alert["body"].count("•"), 3)
        self.assertIn("等 5 条", alert["body"])

        hotspot = _hotspot(payload)
        self.assertEqual(hotspot["total"], 5)
        self.assertEqual(len(hotspot["items"]), 5)
        first, second = hotspot["items"][0], hotspot["items"][1]
        self.assertTrue(first["n"])
        self.assertEqual(first["u"], "https://example.com/0")  # no mobile link: the page's
        self.assertEqual(second["u"], "https://m.example.com/1")  # the mobile link first
        self.assertEqual(first["k"], "科技")
        self.assertEqual(first["s"], "微博")

    def test_mutable_content_lets_the_client_rewrite_the_sound(self):
        """客户端靠它改写提示音，缺了系统不会调用扩展，也不会报错。"""
        ios = _build(_report(1))["notification"]["ios"]
        self.assertIs(ios["mutable-content"], True)
        # 兜底音：扩展没改写时播这个。
        self.assertEqual(ios["sound"], "default")

    def test_duplicates_across_keyword_groups_once(self):
        report = _report(2)
        report["stats"].append(dict(report["stats"][0], word="其他"))
        payload = _build(report)
        self.assertEqual(_hotspot(payload)["total"], 2)

    def test_platform_lists_follow_the_keyword_matches(self):
        standalone = {"platforms": [{"id": "toutiao", "name": "今日头条", "items": [
            {"title": "头条一", "url": "https://toutiao.com/1", "mobileUrl": "https://m.toutiao.com/1"},
            {"title": "热点0字字字字字字字字字字", "url": "https://example.com/0", "mobileUrl": ""},
        ]}]}
        payload = _build(_report(1), standalone_data=standalone)
        items = _hotspot(payload)["items"]
        self.assertEqual([item["k"] for item in items], ["科技", "今日头条"])
        self.assertEqual(items[1]["u"], "https://m.toutiao.com/1")
        self.assertEqual(items[1]["s"], "今日头条")

    def test_stays_within_jpushs_ios_limit(self):
        payload = _build(_report(200, title_length=30), "全天汇总")
        self.assertLessEqual(_ios_size(payload), JPUSH_IOS_LIMIT)
        hotspot = _hotspot(payload)
        self.assertEqual(hotspot["total"], 200)
        self.assertLess(len(hotspot["items"]), 200)
        self.assertGreater(len(hotspot["items"]), 0)


    def test_the_log_shows_what_the_phone_gets(self):
        payload = _build(_report(2))
        text = describe_jpush_payload(payload)
        self.assertIn("标题：热点 · 当前榜单", text)
        self.assertIn("发给：广播（所有设备）", text)
        self.assertIn("发给：标签 hotspot", describe_jpush_payload(_build(_report(2), tag="hotspot")))
        self.assertIn("环境：开发", text)
        self.assertIn("附带条目：2/2", text)
        self.assertIn("1. [科技] 热点0字字字字字字字字字字（微博） 🆕", text)
        self.assertIn("https://m.example.com/1", text)

    def test_hotlist_entries_use_the_camelcase_mobile_link(self):
        """热榜条目由 analyzer 写入的是 mobileUrl，不是 mobile_url。

        两种写法都要读到，否则手机上打开的是 PC 页面——之前的实现只读
        snake_case，而真实热榜数据是驼峰，于是永远回退到 PC 链接。
        """
        report = {
            "stats": [
                {
                    "word": "科技",
                    "count": 1,
                    "titles": [
                        {
                            "title": "热榜里的条目",
                            "source_name": "微博",
                            "url": "https://pc.example.com/1",
                            "mobileUrl": "https://m.example.com/1",
                            "is_new": False,
                        }
                    ],
                }
            ]
        }
        payload = _build(report, "全天汇总")
        self.assertEqual(_hotspot(payload)["items"][0]["u"], "https://m.example.com/1")

    def test_the_environment_switch(self):
        for value in ("true", "TRUE", "1", "yes", "on", True):
            self.assertTrue(parse_jpush_production(value), value)
        for value in ("false", "0", "", None, "no", False):
            self.assertFalse(parse_jpush_production(value), value)


class JPushSendTests(unittest.TestCase):
    def _response(self, status, body):
        response = mock.Mock(status_code=status, text=json.dumps(body))
        response.json.return_value = body
        return response

    def test_sends_with_basic_auth(self):
        with mock.patch("trendradar.notification.senders.requests.post",
                        return_value=self._response(200, {"sendno": "0", "msg_id": "123"})) as post:
            self.assertTrue(send_to_jpush("key", "secret", None, False, _report(1), "当前榜单"))
        args, kwargs = post.call_args
        self.assertEqual(args[0], "https://api.jpush.cn/v3/push")
        self.assertEqual(kwargs["auth"], ("key", "secret"))
        self.assertEqual(kwargs["json"]["audience"], "all")

    def test_a_refusal_is_a_failure(self):
        body = {"error": {"code": 1011, "message": "cannot find user by this audience"}}
        with mock.patch("trendradar.notification.senders.requests.post", return_value=self._response(400, body)):
            self.assertFalse(send_to_jpush("key", "secret", None, False, _report(1), "当前榜单"))

    def test_a_tag_push_refused_as_vip_says_so(self):
        body = {"error": {"code": 2009, "message": "The request is restricted"}}
        with mock.patch("trendradar.notification.senders.requests.post", return_value=self._response(403, body)), \
                mock.patch("builtins.print") as printed:
            self.assertFalse(send_to_jpush("key", "secret", "hotspot", False, _report(1), "当前榜单"))
        logged = " ".join(str(call.args[0]) for call in printed.call_args_list if call.args)
        self.assertIn("JPUSH_TAG 留空改用广播", logged)

    def test_nothing_to_push_sends_nothing(self):
        with mock.patch("trendradar.notification.senders.requests.post") as post:
            self.assertTrue(send_to_jpush("key", "secret", None, False, {"stats": []}, "当前榜单"))
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
