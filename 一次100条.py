import time
import hashlib
import requests
import random
import pandas as pd
import os
import re

# ==================== 账号配置（可随时新增） ====================
ACCOUNTS = [
    {
        "name": "kk",
        "CLIENT_ID": "98a8c70b50294461bd62930e9d9686cc",
        "CLIENT_SECRET": "f7bd3900d6def8e9d12dd198cb7838eedb1d527b",
        "PID": "43512226_310483284"
    },
    {
        "name": "zzz",
        "CLIENT_ID": "dc2351f5456c44f38d18cb0c2d58c519",
        "CLIENT_SECRET": "d25a5cb402973c2582e8d5ee09895e92535f399d",
        "PID": "43527621_310596310"
    }
]

# ==================== 查询关键词 ====================
RAW_KEYWORDS = [
    "金典",
    "安慕希 酸奶",
    "纯牛奶 伊利",
    "高钙奶 伊利",
    "早餐奶 臻浓",
    "优酸乳",
    "舒化",
    "QQ星",
    "谷粒多",
    "甜味奶",
    "畅意",
    "伊刻",
    "植选"
]

def clean_kw(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"[。！？；，、]", " ", s)  # 去常见中文符号
    s = re.sub(r"\s+", " ", s)
    return s

KEYWORDS = [clean_kw(k) for k in RAW_KEYWORDS if k and k.strip()]

# ==================== 常量配置 ====================
PDD_API_URL = "https://gw-api.pinduoduo.com/api/router"
CUSTOM_PARAMETERS = '{"uid":"demo","source":"test"}'
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "pdd_goods_realtime.xlsx")
os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE = 1          # 每个账号只查一页
PAGE_SIZE = 100   # 每页 100 条

# ==================== 工具函数 ====================
def sign(params: dict, secret: str) -> str:
    items = sorted(params.items())
    raw = secret + "".join(f"{k}{v}" for k, v in items) + secret
    return hashlib.md5(raw.encode("utf-8")).hexdigest().upper()

def search_goods(account: dict, keyword: str, page: int = PAGE, page_size: int = PAGE_SIZE) -> dict:
    params = {
        "type": "pdd.ddk.goods.search",
        "client_id": account["CLIENT_ID"],
        "timestamp": int(time.time()),
        "data_type": "JSON",
        "keyword": keyword,
        "pid": account["PID"],
        "custom_parameters": CUSTOM_PARAMETERS,
        "page": page,
        "page_size": page_size,
        # 若需要固定排序，可按文档增加：
        # "sort_type": 0
    }
    params["sign"] = sign(params, account["CLIENT_SECRET"])
    r = requests.post(PDD_API_URL, data=params, timeout=20)
    return r.json()

def append_to_excel(data: list):
    """将新增数据合并写盘（基于 goods_id 全量去重）"""
    if not data:
        return
    df_new = pd.DataFrame(data)
    if not os.path.exists(OUTPUT_FILE):
        df_new.to_excel(OUTPUT_FILE, index=False)
        print(f"💾 首次创建文件，写入 {len(df_new)} 条。")
        return
    df_old = pd.read_excel(OUTPUT_FILE)
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=["goods_id"], inplace=True)
    df_all.to_excel(OUTPUT_FILE, index=False)
    print(f"📈 已追加 {len(df_new)} 条，当前总 {len(df_all)} 条。")

# ==================== 主逻辑 ====================
def main():
    print("🔎 按关键词批量查询（每账号只查一页，每页100条；限流即禁用账号并跳过；同一商品全局只保留一次）\n")
    print("关键词列表：", ", ".join(KEYWORDS))
    print("-" * 60)

    # 账号状态：支持动态禁用（被限流即设为 False）
    account_enabled = {acc["name"]: True for acc in ACCOUNTS}

    # 全局商品去重（跨账号、跨关键词）
    global_seen_ids = set()

    total_new = 0
    for kw in KEYWORDS:
        print(f"\n🟩 开始关键词：『{kw}』")
        kw_new = 0

        for account in ACCOUNTS:
            name = account["name"]
            if not account_enabled.get(name, True):
                print(f"⛔ 账号【{name}】已禁用（本轮不再使用），跳过。")
                continue

            print(f"➡️ 账号【{name}】查询：第 {PAGE} 页 × {PAGE_SIZE} 条")
            # 轻微抖动，降低频控
            time.sleep(random.uniform(1.5, 3.5))

            try:
                res = search_goods(account, kw, page=PAGE, page_size=PAGE_SIZE)
            except Exception as e:
                print(f"   ❌ 请求异常：{e}（跳过该账号本关键词）")
                continue

            # 错误处理：遇到 40009 直接禁用该账号
            if "error_response" in res:
                err = res["error_response"]
                sub = err.get("sub_code")
                print(f"   ⚠️ 接口异常：{err}")
                if sub == "40009":
                    account_enabled[name] = False
                    print(f"   🚫 账号【{name}】触发限流，已标记禁用，改用其它账号。")
                # 其他错误：跳过该账号本关键词
                continue

            rsp = res.get("goods_search_response", {})
            goods_list = rsp.get("goods_list", []) or []
            if not goods_list:
                print("   ℹ️ 无返回结果。")
                continue

            batch_new = []
            for g in goods_list:
                gid = str(g.get("goods_id"))
                if gid in global_seen_ids:
                    continue
                global_seen_ids.add(gid)
                batch_new.append({
                    "keyword": kw,
                    "account": name,
                    "goods_id": gid,
                    "goods_name": (g.get("goods_name") or "")[:120],
                    "price": (g.get("min_group_price") or 0) / 100,
                    "mall_id": g.get("mall_id")
                })

            if batch_new:
                append_to_excel(batch_new)
                kw_new += len(batch_new)
                total_new += len(batch_new)
                print(f"   ✅ 新增 {len(batch_new)} 条。关键词累计 {kw_new} 条。")
            else:
                print("   ↪️ 本次没有新增（均为已收录商品）。")

        print(f"🏁 关键词『{kw}』完成，新增 {kw_new} 条。")
        print("-" * 60)

    print(f"\n🎉 全部完成！共新增 {total_new} 条。输出文件：{OUTPUT_FILE}")
    # 可选：列出最终被禁用的账号，方便你后续观察与扩容
    disabled = [n for n, en in account_enabled.items() if not en]
    if disabled:
        print("🚫 本轮触发限流并被禁用的账号：", ", ".join(disabled))
    else:
        print("✅ 本轮无账号被限流禁用。")

# ==================== 入口 ====================
if __name__ == "__main__":
    main()

# 一次100条.py 末尾追加
def run_fetch():
    """
    对外调用：执行“每个账号只查一页、每页100条”的抓取流程，
    结果写入 output/pdd_goods_realtime.xlsx（文件内已去重）。
    """
    main()

