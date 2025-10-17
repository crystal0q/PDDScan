import pandas as pd

input_path = "output/filtered_result.xlsx"
output_path = "output/filtered_result.xlsx"

# 读取文件
df = pd.read_excel(input_path)
print(f"去重前共有 {len(df)} 条记录")

# 去重：按 goods_id 去掉重复项（保留第一条）
df = df.drop_duplicates(subset=["goods_id"])
print(f"✅ 去重后共有 {len(df)} 条记录")

# 检查必要字段是否存在
required_cols = ["品类", "商品名称", "差额比"]
for c in required_cols:
    if c not in df.columns:
        raise ValueError(f"❌ 缺少必要列：{c}")

# 删除条件 1️⃣：品类是纯奶 且 商品名称不含 “伊利”
mask_puremilk = (df["品类"] == "纯奶") & (~df["商品名称"].str.contains("伊利", na=False))
df = df[~mask_puremilk]

# 删除条件 2️⃣：差额比 > 50%
# 差额比是字符串形式如 "35.2%"，需要先提取数字
df["差额比数值"] = df["差额比"].astype(str).str.replace("%", "", regex=False)
df["差额比数值"] = pd.to_numeric(df["差额比数值"], errors="coerce")
before = len(df)
df = df[df["差额比数值"] <= 50]
deleted = before - len(df)

# 保存结果
df.drop(columns=["差额比数值"], inplace=True)
df.to_excel(output_path, index=False)

print(f"✅ 已删除差额比大于 50% 的记录，共删除 {deleted} 行。")
print(f"📄 新文件已保存到：{output_path}")
# 去掉差额比大于50.py 顶部或底部新增
def drop_ratio_gt_50_inplace(path: str = "output/filtered_result.xlsx") -> None:
    df = pd.read_excel(path)
    df = df.drop_duplicates(subset=["goods_id"])  # 保险再去一次重
    # 差额比 "35.2%" -> 数字
    df["差额比数值"] = pd.to_numeric(
        df["差额比"].astype(str).str.replace("%", "", regex=False),
        errors="coerce"
    )
    df = df[df["差额比数值"] <= 50]
    df.drop(columns=["差额比数值"], inplace=True)
    df.to_excel(path, index=False)

