# -*- coding: utf-8 -*-
"""
一键流水线：
1) 抓数（每账号只查一页×100条）-> output/pdd_goods_realtime.xlsx
2) 价格比对（低于控价）        -> output/filtered_result.xlsx
3) 清洗（差额比>50%剔除 + 去重) -> 覆写 output/filtered_result.xlsx
"""
import os
import datetime
import re
import time
import random
import hashlib
import requests
import pandas as pd
import os, sys
BASE = getattr(sys, '_MEIPASS', os.path.abspath(os.path.dirname(__file__)))
PRICE_PATH = os.path.join(BASE, 'price.xlsx')
# =============【A. 抓数：一次100条】=========================================
ACCOUNTS = [
    {"name": "kk",  "CLIENT_ID": "98a8c70b50294461bd62930e9d9686cc",
     "CLIENT_SECRET": "f7bd3900d6def8e9d12dd198cb7838eedb1d527b", "PID": "43512226_310483284"},
    {"name": "zzz", "CLIENT_ID": "dc2351f5456c44f38d18cb0c2d58c519",
     "CLIENT_SECRET": "d25a5cb402973c2582e8d5ee09895e92535f399d", "PID": "43527621_310596310"},
]
RAW_KEYWORDS = [
    "金典 牛奶","安慕希 酸奶","纯牛奶 伊利","高钙奶 伊利","早餐奶 臻浓",
    "优酸乳","舒化","QQ星","谷粒多","甜味奶","畅意","伊刻 植选"
]
# RAW_KEYWORDS = [
#     "金典","安慕希 酸奶"
# ]
def _clean_kw(s:str)->str:
    s=str(s).strip()
    s=re.sub(r"[。！？；，、]", " ", s)
    s=re.sub(r"\s+", " ", s)
    return s
KEYWORDS=[_clean_kw(k) for k in RAW_KEYWORDS if k and k.strip()]

PDD_API_URL="https://gw-api.pinduoduo.com/api/router"
CUSTOM_PARAMETERS='{"uid":"demo","source":"test"}'
OUTPUT_DIR="output"
GOODS_FILE=os.path.join(OUTPUT_DIR,"pdd_goods_realtime.xlsx")
os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE=1
PAGE_SIZE=100

def _sign(params:dict, secret:str)->str:
    items=sorted(params.items())
    raw=secret + "".join(f"{k}{v}" for k,v in items) + secret
    return hashlib.md5(raw.encode("utf-8")).hexdigest().upper()

def _search_goods(account:dict, keyword:str, page:int=PAGE, page_size:int=PAGE_SIZE)->dict:
    params={
        "type":"pdd.ddk.goods.search",
        "client_id":account["CLIENT_ID"],
        "timestamp":int(time.time()),
        "data_type":"JSON",
        "keyword":keyword,
        "pid":account["PID"],
        "custom_parameters":CUSTOM_PARAMETERS,
        "page":page,
        "page_size":page_size,
        # "sort_type":0,  # 如需固定排序可启用
    }
    params["sign"]=_sign(params, account["CLIENT_SECRET"])
    r=requests.post(PDD_API_URL, data=params, timeout=20)
    return r.json()

def _append_goods_rows(rows:list):
    if not rows:
        return
    df_new=pd.DataFrame(rows)
    if not os.path.exists(GOODS_FILE):
        df_new.to_excel(GOODS_FILE, index=False)
        print(f"💾 创建: {GOODS_FILE}（{len(df_new)}条）")
        return
    df_old=pd.read_excel(GOODS_FILE)
    df_all=pd.concat([df_old, df_new], ignore_index=True)
    df_all.drop_duplicates(subset=["goods_id"], inplace=True)
    df_all.to_excel(GOODS_FILE, index=False)
    print(f"📈 追加 {len(rows)} 条，现共 {len(df_all)} 条")

def run_fetch():
    print("▶ A. 抓数（轮询账号：每关键词只用一个账号；限流账号即时禁用）")
    # 可用状态表
    account_enabled = {a["name"]: True for a in ACCOUNTS}
    # 当前轮询指针
    idx = 0  # 指向下一个优先尝试的账号下标
    n_accounts = len(ACCOUNTS)

    seen = set()   # 已见 goods_id
    total = 0

    def next_enabled(start_idx):
        """从 start_idx 起向前找第一个可用账号；若全不可用返回 None"""
        if n_accounts == 0:
            return None
        for step in range(n_accounts):
            j = (start_idx + step) % n_accounts
            if account_enabled.get(ACCOUNTS[j]["name"], False):
                return j
        return None

    for kw in KEYWORDS:
        print(f"\n🔎 关键词：{kw}")

        # 找到一个可用账号来处理本关键词
        j = next_enabled(idx)
        if j is None:
            print("⛔ 所有账号均不可用，提前结束抓数。")
            break

        tried = 0
        kw_new = 0
        success = False

        while tried < n_accounts:
            acc = ACCOUNTS[j]
            if not account_enabled.get(acc["name"], True):
                tried += 1
                j = (j + 1) % n_accounts
                continue

            # 轻微随机延迟，降低触发率
            time.sleep(random.uniform(1.5, 3.5))
            print(f"→ 使用账号查询 1 页 × {PAGE_SIZE} 条")

            try:
                res = _search_goods(acc, kw, page=PAGE, page_size=PAGE_SIZE)
            except Exception as e:
                print(f"❌ {acc['name']} 请求异常：{e}")
                tried += 1
                j = (j + 1) % n_accounts
                continue

            if "error_response" in res:
                err = res["error_response"]
                sub = err.get("sub_code")
                print(f"⚠️ {acc['name']} 接口异常：{err}")
                if sub == "40009":
                    # 即时禁用本账号，换下一个账号继续尝试本关键词
                    account_enabled[acc["name"]] = False
                    print(f"🚫 账号已被限流，标记为不可用。")
                tried += 1
                j = (j + 1) % n_accounts
                continue

            goods = (res.get("goods_search_response", {}) or {}).get("goods_list", []) or []
            batch = []
            for g in goods:
                gid = str(g.get("goods_id"))
                if gid in seen:
                    continue
                seen.add(gid)
                batch.append({
                    "keyword": kw,
                    "account": acc["name"],
                    "goods_id": gid,
                    "goods_name": (g.get("goods_name") or "")[:120],
                    "price": (g.get("min_group_price") or 0) / 100,
                    "mall_id": g.get("mall_id"),
                })

            if batch:
                _append_goods_rows(batch)
                total += len(batch)
                kw_new += len(batch)
                print(f"✅ 关键词『{kw}』由账号新增 {len(batch)} 条")
            else:
                print(f"↪️ 账号本次无新增")

            success = True
            break  # 本关键词已由某账号完成，跳出 while

        if not success:
            print(f"❗ 关键词『{kw}』未能成功查询（账号都不可用或异常）。")

        # 将轮询指针挪到“下一个账号”，用于分配下一个关键词
        j2 = next_enabled((idx + 1) % n_accounts)
        idx = j2 if j2 is not None else idx  # 若全不可用就保持不动

        print(f"🏁 关键词完成：新增 {kw_new} 条")

    print(f"\n📁 抓数完成，累计新增 {total} 条 → {GOODS_FILE}")

# =============【B. 比价：生成低于控价清单】===============================
PRICE_FILE="price.xlsx"   # sheet: 电商控价
RESULT_FILE=os.path.join(OUTPUT_DIR, "filtered_result.xlsx")

def _extract_pack_count(name:str)->int:
    text=str(name); counts=[]
    for a,b,unit in re.findall(r'(\d+)\s*[x×X]\s*(\d+)\s*(箱|提)', text):
        try: counts.append(int(a)*int(b))
        except: pass
    for unit in ['箱','提']:
        try:
            found=re.findall(r'(?<!\d)(\d+)\s*'+unit, text)
            counts.extend(int(n) for n in found)
        except: pass
    return max(1, min(counts)) if counts else 1

def _is_expiring(name:str)->bool:
    for m in re.findall(r'(\d{1,2})月', str(name)):
        try:
            if int(m)<=7: return True
        except: pass
    return False

def _extract_primary_size(text:str):
    if text is None: return (None,None)
    s=str(text)
    m=re.search(r'(\d+(?:\.\d+)?)\s*(ml|mL|ML|l|L|g|G)', s)
    if not m: return (None,None)
    val=float(m.group(1)); unit=m.group(2).lower()
    if unit in ('l',): return (val*1000.0, 'ml')
    elif unit in ('ml',): return (val, 'ml')
    else: return (val, 'g')

def _choose_best_by_spec(matched_df:pd.DataFrame, goods_name:str)->pd.Series:
    gv, gu = _extract_primary_size(goods_name)
    if gv is None or gu is None or "规格" not in matched_df.columns:
        return matched_df.iloc[0]
    tmp=matched_df.copy()
    spec_vals=[]; spec_units=[]
    for x in tmp["规格"] if "规格" in tmp.columns else [None]*len(tmp):
        v,u=_extract_primary_size(x); spec_vals.append(v); spec_units.append(u)
    tmp["_spec_val"]=spec_vals; tmp["_spec_unit"]=spec_units
    same_unit = tmp["_spec_unit"]==gu
    if same_unit.any():
        sub=tmp[same_unit].copy()
        sub["_spec_diff"]=(sub["_spec_val"]-gv).abs()
        sub=sub.sort_values(["_spec_diff"], kind="mergesort")
        return sub.iloc[0]
    return matched_df.iloc[0]

def run_compare():
    print("\n▶ B. 价格比对（低于控价）")
    if not os.path.exists(PRICE_FILE):
        raise FileNotFoundError(f"控价文件不存在：{PRICE_FILE}")
    if not os.path.exists(GOODS_FILE):
        raise FileNotFoundError(f"商品文件不存在：{GOODS_FILE}")

    df_price=pd.read_excel(PRICE_FILE, sheet_name="电商控价")
    df_goods=pd.read_excel(GOODS_FILE)

    # 统一列名
    rename_map={"商品名称":"goods_name","商品名":"goods_name","goods_name":"goods_name",
                "商品ID":"goods_id","goodsId":"goods_id",
                "价格":"price","当前售价":"price","mall_id":"mall_id"}
    for old,new in rename_map.items():
        if old in df_goods.columns and new not in df_goods.columns:
            df_goods.rename(columns={old:new}, inplace=True)

    # 清洗控价
    for col in ["促销指引价","效期产品控价（单提）"]:
        if col in df_price.columns:
            df_price[col]=df_price[col].astype(str).apply(
                lambda x: float(re.sub(r"[^\d.]","",x)) if re.search(r"\d",x) else None
            )
    for col in ["品类","品项名称","产品简称"]:
        if col not in df_price.columns: df_price[col]=""

    ban_keywords=["蒙牛","新希望","认养"]

    results=[]
    for idx,row in df_goods.iterrows():
        try:
            name=str(row.get("goods_name","")).strip()
            price=float(row.get("price",0))
        except Exception as e:
            print(f"⚠️ 第{idx}行读取异常：{e}"); continue
        if any(k in name for k in ban_keywords):
            continue
        matched=df_price[
            (df_price["品项名称"].apply(lambda x: str(x) in name if not pd.isna(x) else False)) |
            (df_price["产品简称"].apply(lambda x: str(x) in name if not pd.isna(x) else False))
            ]
        if matched.empty: continue

        p=_choose_best_by_spec(matched, name)
        exp=_is_expiring(name)
        compare_type="效期控价" if exp else "促销指引价"
        base_col='效期产品控价（单提）' if exp else '促销指引价'
        base=float(p.get(base_col,0) or 0)

        pack=_extract_pack_count(name)
        compare_price=base*pack

        try:
            if float(price) < float(compare_price):
                diff=compare_price-price
                diff_ratio=(diff/compare_price*100) if compare_price!=0 else 0
                results.append({
                    "mall_id": row.get("mall_id",""),
                    "goods_id": row.get("goods_id",""),
                    "商品名称": name,
                    "品类": p.get("品类",""),
                    "品项名称": p.get("品项名称",""),
                    "规格": p.get("规格","") if "规格" in p else "",
                    "当前售价": round(price,2),
                    "比对类型": compare_type,
                    "单提控价": round(base,2),
                    "倍数": pack,
                    "调整后控价": round(compare_price,2),
                    "差额": round(diff,2),
                    "差额比": f"{diff_ratio:.1f}%",
                    "是否效期商品": "是" if exp else "否",
                    "链接": f"https://mobile.yangkeduo.com/goods.html?goods_id={row.get('goods_id','')}"
                })
        except Exception as e:
            print(f"⚠️ 比较时出错：{e}")
            continue

    # 安全创建目录
    dir_name=os.path.dirname(RESULT_FILE)
    if dir_name: os.makedirs(dir_name, exist_ok=True)

    if not results:
        print("⚠️ 没有低于控价的商品。")
        # 仍然写一个空表，便于后续清洗步骤统一处理
        pd.DataFrame(columns=[
            "mall_id","goods_id","商品名称","品类","品项名称","规格","当前售价",
            "比对类型","单提控价","倍数","调整后控价","差额","差额比","是否效期商品","链接"
        ]).to_excel(RESULT_FILE, index=False)
    else:
        df_res=pd.DataFrame(results)
        with pd.ExcelWriter(RESULT_FILE, engine="openpyxl") as w:
            df_res.to_excel(w, index=False, sheet_name="结果")
        print(f"✅ 比价完成：{len(df_res)} 条 → {RESULT_FILE}")

# =============【C. 清洗：差额比>50%剔除 + 去重】=========================
def run_cleanup():
    print("\n▶ C. 清洗")
    if not os.path.exists(RESULT_FILE):
        print("⚠️ 未找到比对结果，跳过清洗")
        return

    df = pd.read_excel(RESULT_FILE)
    print(f"原始：{len(df)} 条")

    # ① 去重（按 goods_id）
    if "goods_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["goods_id"])
        print(f"去重：移除 {before - len(df)} 条，当前 {len(df)} 条")

    # ② 去掉 差额 < 0
    if "差额" in df.columns:
        df["差额"] = pd.to_numeric(df["差额"], errors="coerce")
        before = len(df)
        df = df[df["差额"] >= 0]
        print(f"差额<0：移除 {before - len(df)} 条，当前 {len(df)} 条")
    else:
        print("⚠️ 缺少“差额”列，跳过差额<0过滤")

    # ③ 去掉 指定品类 且 商品名称包含“伊利”
    target_categories = ["纯奶", "功能奶", "早餐奶", "臻浓", "甜味奶", "草原酸奶", "花色奶"]
    if ("品类" in df.columns) and ("商品名称" in df.columns):
        # 注意：这里是“包含伊利”的剔除（与你这次描述一致）
        mask = df["品类"].isin(target_categories) & (~df["商品名称"].astype(str).str.contains("伊利", na=False))
        removed = int(mask.sum())
        df = df[~mask]
        print(f"指定品类且含“伊利”：移除 {removed} 条，当前 {len(df)} 条")
    else:
        print("⚠️ 缺少“品类”或“商品名称”列，跳过该规则")

    # ④ 去掉 差额比 > 50%
    if "差额比" in df.columns:
        df["差额比数值"] = pd.to_numeric(
            df["差额比"].astype(str).str.replace("%", "", regex=False),
            errors="coerce"
        )
        before = len(df)
        df = df[df["差额比数值"] <= 50]
        removed = before - len(df)
        df.drop(columns=["差额比数值"], inplace=True)
        print(f"差额比>50%：移除 {removed} 条，当前 {len(df)} 条")
    else:
        print("⚠️ 缺少“差额比”列，跳过差额比过滤")
        # === 🆕 表头重命名 ===
    df.rename(columns={
        "商品名称": "买货产品名称",
        "链接": "买货链接",
        "当前售价": "买货价格（元/件）"
    }, inplace=True)

    # === 🆕 删除不需要的列 mall_id	goods_id 比对类型	单提控价	倍数	调整后控价	差额	差额比	是否效期商品===
    drop_cols = [
        "mall_id", "goods_id", "比对类型", "单提控价", "倍数", "调整后控价", "差额", "差额比", "是否效期商品"
    ]
    df.drop(columns=drop_cols, inplace=True, errors='ignore')

    # === 🆕 增加“平台名称”列 品类	品项名称	规格 ===
    df["平台名称"] = "拼多多"
    df["批次反馈日期"] = datetime.date.today().strftime("%Y-%m-%d")
    desired_order = [
        "平台名称", "买货链接", "批次反馈日期", "买货产品名称", "品类", "品项名称", "规格", "买货价格（元/件）"
    ]
    # 写回文件
    df.to_excel(RESULT_FILE, index=False)
    print(f"📄 清洗完成并修改表头，已覆写：{RESULT_FILE}")


    # 写回
    df.to_excel(RESULT_FILE, index=False)
    print(f"📄 清洗完成，已覆写：{RESULT_FILE}")


# =============【工具函数：是否清空旧数据】=========================

def ask_clear_raw_data():  # 🟢 新增
    """询问是否清空 output/pdd_goods_realtime.xlsx"""
    if os.path.exists(GOODS_FILE):
        ans = input(f"是否清空原始数据文件 {GOODS_FILE}? (y/N): ").strip().lower()
        if ans == "y":
            os.remove(GOODS_FILE)
            print("🧹 已清空旧的抓取数据文件。")
        else:
            print("✅ 保留现有抓取数据。")
    else:
        print("ℹ️ 当前不存在旧的抓取文件，无需清理。")

# =============【Main 主流程】========================================

if __name__ == "__main__":
    ask_clear_raw_data()  # 🟢 新增：运行前询问是否清空旧文件
    run_fetch()           # A. 抓数
    run_compare()         # B. 比对
    run_cleanup()         # C. 清洗
    print("\n🎉 全流程完成")

