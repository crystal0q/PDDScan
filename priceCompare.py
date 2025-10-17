import pandas as pd
import re
import os

# ============================================================
# 📘 文件路径（按需修改）
# ============================================================
price_path = r"price.xlsx"   # 电商控价文件
goods_path = r"output\pdd_goods_realtime.xlsx"  # 抓取结果文件
out_path = r"output\filtered_result.xlsx"


# ============================================================
# 🧩 检查文件是否存在
# ============================================================
if not os.path.exists(price_path):
    raise FileNotFoundError(f"❌ 控价文件不存在: {price_path}")

if not os.path.exists(goods_path):
    raise FileNotFoundError(f"❌ 商品文件不存在: {goods_path}")

# ============================================================
# 🧾 读取文件
# ============================================================
df_price = pd.read_excel(price_path, sheet_name="电商控价")
df_goods = pd.read_excel(goods_path)

# ============================================================
# 🧹 统一列名（兼容中文英文列）
# ============================================================
rename_map = {
    "商品名称": "goods_name",
    "商品名": "goods_name",
    "goods_name": "goods_name",
    "商品ID": "goods_id",
    "goodsId": "goods_id",
    "价格": "price",
    "当前售价": "price",
    "mall_id": "mall_id"
}
for old, new in rename_map.items():
    if old in df_goods.columns and new not in df_goods.columns:
        df_goods.rename(columns={old: new}, inplace=True)

# ============================================================
# 🧮 清洗控价数据（去掉非数字字符）
# ============================================================
for col in ["促销指引价", "效期产品控价（单提）"]:
    if col in df_price.columns:
        df_price[col] = df_price[col].astype(str).apply(
            lambda x: float(re.sub(r"[^\d.]", "", x)) if re.search(r"\d", x) else None
        )

# 保险起见，关键列缺失用空串/None 填充
for col in ["品类", "品项名称", "产品简称"]:
    if col not in df_price.columns:
        df_price[col] = ""

# ============================================================
# 🧠 辅助函数定义
# ============================================================
def extract_pack_count(name: str) -> int:
    """
    识别箱/提倍数：若出现多个“箱/提”数量，取最小值；支持 2x6箱/2×6提；默认返回 1
    """
    text = str(name)
    counts = []

    # 形式：2x6箱 / 2×6提
    for a, b, unit in re.findall(r'(\d+)\s*[x×X]\s*(\d+)\s*(箱|提)', text):
        try:
            counts.append(int(a) * int(b))
        except Exception:
            pass

    # 单独：6箱 / 12提（出现多次时全部收集）
    for unit in ['箱', '提']:
        try:
            found = re.findall(r'(?<!\d)(\d+)\s*' + unit, text)
            counts.extend(int(n) for n in found)
        except Exception:
            pass

    # 多个数量同时存在时，按最小值处理；未识别则返回 1
    if counts:
        return max(1, min(counts))
    return 1


def is_expiring(name: str) -> bool:
    """识别效期商品：名称出现 'X月' 且 X<=7 则视为效期"""
    for m in re.findall(r'(\d{1,2})月', str(name)):
        try:
            if int(m) <= 7:
                return True
        except Exception:
            continue
    return False


def _extract_primary_size(text: str):
    """
    从文本中提取“单位规格”的数值与单位，用于规格匹配：
      - 匹配单个单位：ml/mL/ML/L/l/g/G
      - 复合写法：250ml×12 / 1L*6 / 200gX10 —— 只取“单位规格”数值（250ml / 1L / 200g）
    返回 (value, unit_type)
      - unit_type: 'ml' 或 'g'；L 会换算为 ml；若无可用规格则返回 (None, None)
    """
    if text is None:
        return (None, None)
    s = str(text)

    # 找到第一个带体积/重量单位的片段
    m = re.search(r'(\d+(?:\.\d+)?)\s*(ml|mL|ML|l|L|g|G)', s)
    if not m:
        return (None, None)

    val = float(m.group(1))
    unit = m.group(2).lower()

    if unit in ('l',):
        # L -> ml
        return (val * 1000.0, 'ml')
    elif unit in ('ml',):
        return (val, 'ml')
    else:
        # g
        return (val, 'g')


def choose_best_by_spec(matched_df: pd.DataFrame, goods_name: str) -> pd.Series:
    """
    在候选 matched_df 中，结合“规格”列与商品名规格，挑选与商品规格最接近的一行。
    规则：
      - 若商品名解析不到规格，或价格表无“规格”列，则直接返回第一行；
      - 若能解析，则将“规格”列解析成 (value, unit_type)，
        仅在 unit_type 一致时比较差值，选差值最小的行；
      - 若所有解析失败或单位不一致，仍回退第一行。
    """
    goods_val, goods_unit = _extract_primary_size(goods_name)
    if goods_val is None or goods_unit is None or "规格" not in matched_df.columns:
        return matched_df.iloc[0]

    tmp = matched_df.copy()
    # 解析控价表“规格”
    spec_vals = []
    spec_units = []
    for x in tmp["规格"] if "规格" in tmp.columns else [None] * len(tmp):
        v, u = _extract_primary_size(x)
        spec_vals.append(v)
        spec_units.append(u)
    tmp["_spec_val"] = spec_vals
    tmp["_spec_unit"] = spec_units

    # 只比较单位一致的
    same_unit = tmp["_spec_unit"] == goods_unit
    if same_unit.any():
        sub = tmp[same_unit].copy()
        sub["_spec_diff"] = (sub["_spec_val"] - goods_val).abs()
        sub = sub.sort_values(["_spec_diff"], kind="mergesort")
        return sub.iloc[0]
    else:
        # 单位都不一致或无法解析，回退
        return matched_df.iloc[0]

# ============================================================
# 🚫 过滤关键词（品牌）
# ============================================================
ban_keywords = ["蒙牛", "新希望", "认养"]

# ============================================================
# 🏁 主逻辑：比价
# ============================================================
results = []

for idx, row in df_goods.iterrows():
    try:
        name = str(row.get('goods_name', '')).strip()
        price = float(row.get('price', 0))
    except Exception as e:
        print(f"⚠️ 第{idx}行读取异常：{e}")
        continue

    # ⛔ 过滤名字中含有指定字样的记录
    if any(k in name for k in ban_keywords):
        continue

    # ① 大致匹配：品项名称 / 产品简称 作为候选
    matched = df_price[
        (df_price['品项名称'].apply(lambda x: str(x) in name if not pd.isna(x) else False)) |
        (df_price['产品简称'].apply(lambda x: str(x) in name if not pd.isna(x) else False))
        ]

    if matched.empty:
        continue

    # ② 规格细化：根据“规格”列的数字与商品名解析的规格最接近选择一条
    p = choose_best_by_spec(matched, name)

    # ③ 效期/价格/倍数
    expiring = is_expiring(name)
    compare_type = "效期控价" if expiring else "促销指引价"
    base_price_col = '效期产品控价（单提）' if expiring else '促销指引价'
    base_price = float(p.get(base_price_col, 0) or 0)

    pack = extract_pack_count(name)
    compare_price = base_price * pack

    try:
        if float(price) < float(compare_price):
            diff = compare_price - price
            diff_ratio = (diff / compare_price * 100) if compare_price != 0 else 0

            results.append({
                "mall_id": row.get("mall_id", ""),
                "goods_id": row.get("goods_id", ""),
                "商品名称": name,
                "品类": p.get("品类", ""),
                "品项名称": p.get("品项名称", ""),
                "规格": p.get("规格", "") if "规格" in p else "",
                "当前售价": round(price, 2),
                "比对类型": compare_type,
                "单提控价": round(base_price, 2),
                "倍数": pack,
                "调整后控价": round(compare_price, 2),
                "差额": round(diff, 2),
                "差额比": f"{diff_ratio:.1f}%",
                "是否效期商品": "是" if expiring else "否",
                "链接": f"https://mobile.yangkeduo.com/goods.html?goods_id={row.get('goods_id', '')}"
            })
    except Exception as e:
        print(f"⚠️ 比较时出错：{e}")
        continue

# ============================================================
# 💾 输出结果
# ============================================================
os.makedirs(os.path.dirname(out_path), exist_ok=True)

if not results:
    print("⚠️ 没有低于控价的商品。")
else:
    df_res = pd.DataFrame(results)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_res.to_excel(writer, index=False, sheet_name="结果")
    print(f"✅ 比价完成，共发现 {len(df_res)} 条低于控价的商品。")
    print(f"📄 文件已生成：{out_path}")

# 价格比对.py 顶部或底部新增
def run_compare(
        price_path: str = "price.xlsx",
        goods_path: str = r"output\pdd_goods_realtime.xlsx",
        out_path: str = r"output\filtered_result.xlsx"
) -> str:
    """
    读取 price.xlsx 与 output/pdd_goods_realtime.xlsx，
    产出低于控价的明细到 output/filtered_result.xlsx。
    返回 out_path 便于后续流程继续处理。
    """
    # —— 下面沿用你文件中的全部现有代码 ——
    # 包含：读取、rename_map、清洗价格列、各辅助函数（extract_pack_count 等）、
    # 主循环比价、写出 out_path 等。
    # 只要把你原文件最外层的执行代码移入这个函数体内即可。
    # （注意：保持列名和逻辑不变）
    return out_path
