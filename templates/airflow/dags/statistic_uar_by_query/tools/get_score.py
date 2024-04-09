#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def clean_action(acts):
    for id, ac in enumerate(acts):
        acts[id] = remove_id_field_helper(ac)
    return acts


def extract_mp3_action(d):
    for ac in d:
        if "action_code" in ac and ac["action_code"] == "PlayerMP3":
            return ac
    if d and "payload" in d[0]:
        return d[0]
    return {}


def extract_text_mp3_action(act):
    text = ""
    if "name" in act["payload"]:
        text = act["payload"]["name"]
    elif (
        "text" in act["payload"]
        and act["payload"]["text"][: len("Mình tìm thấy bài hát ")]
        == "Mình tìm thấy bài hát "
    ):
        text = (
            act["payload"]["text"]
            .split("Mình tìm thấy bài hát ")[-1]
            .split(". Bạn có muốn mở bài này không")[0]
        )
    elif (
        "utter_text" in act["payload"]
        and act["payload"]["utter_text"][: len("Mình tìm thấy bài hát ")]
        == "Mình tìm thấy bài hát "
    ):
        text = (
            act["payload"]["utter_text"]
            .split("Mình tìm thấy bài hát ")[-1]
            .split(". Bạn có muốn mở bài này không")[0]
        )
    # if "artist" in act["payload"] and act["payload"]["artist"]:
    #     text += " của " + act["payload"]["artist"]
    return text


def extract_action(d):
    if "directives" not in d:
        return []
    return d["directives"]


def remove_id_field_helper(ac):
    for kw in ["action_id", "required", "session_key", "request_id", "log_request_id"]:
        if kw in ac.keys():
            ac[kw] = ""

    for kw in ac:
        if type(ac[kw]) == dict:
            ac[kw] = remove_id_field_helper(ac[kw])
    return ac


def cmp_action(act1, act2):
    return act1 == act2
