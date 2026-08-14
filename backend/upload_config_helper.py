# -*- coding: utf-8 -*-
"""上传目录配置（设置页可自定义 NAS 位置）：upload_config.json 存路径，动态读取"""
import json
import os
DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")

DEFAULT_DIR = os.environ.get("QL_UPLOAD_DIR", os.path.join(DATA_DIR, "uploads"))
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "upload_config.json")


def get_dir():
    """读取可配置上传目录；未配置/异常回默认"""
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            d = json.load(f).get("upload_dir", "")
        if d and os.path.isabs(d):
            return d
    except Exception:
        pass
    return DEFAULT_DIR


def set_dir(path):
    """保存上传目录配置（自动创建目录）"""
    p = os.path.normpath(path)
    if not os.path.isabs(p):
        return False, "必须是绝对路径"
    try:
        os.makedirs(p, exist_ok=True)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump({"upload_dir": p}, f, ensure_ascii=False)
        return True, "已保存，新上传将写入该目录"
    except OSError as e:
        return False, str(e)[:120]
