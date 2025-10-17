import pandas as pd

input_path = "output/filtered_result.xlsx"
output_path = "output/filtered_result.xlsx"

# 读取文件
df = pd.read_excel(input_path)

# 打印原始条数
print(f"去重前共有 {len(df)} 条记录")

# 去重：按 goods_id 去掉重复项
df = df.drop_duplicates(subset=["goods_id"])

# 保存
df.to_excel(output_path, index=False)

print(f"✅ 去重后共有 {len(df)} 条记录，已保存到 {output_path}")
df = pd.read_excel(output_path)

# 检查必要字段是否存在
required_cols = ["品类", "商品名称"]
for c in required_cols:
    if c not in df.columns:
        raise ValueError(f"❌ 缺少必要列：{c}")

# 删除条件：品类是纯奶 且 商品名称不含 “伊利”
# 定义要检查的品类列表
target_categories = ["纯奶", "功能奶", "早餐奶", "臻浓", "甜味奶", "草原酸奶", "花色奶"]
# 筛选出这些品类中，商品名称里没有“伊利”的行
mask = df["品类"].isin(target_categories) & (~df["商品名称"].str.contains("伊利", na=False))

df_filtered = df[~mask]  # 取反：保留其他行

# 保存结果

df_filtered.to_excel(output_path, index=False)

print(f"✅ 已删除符合条件的记录，共删除 {len(df) - len(df_filtered)} 行。")
print(f"📄 新文件已保存到：{output_path}")

# 去掉重复的.py 顶部或底部新增
def dedupe_inplace(path: str = "output/filtered_result.xlsx") -> None:
    df = pd.read_excel(path)
    df = df.drop_duplicates(subset=["goods_id"])
    df.to_excel(path, index=False)
