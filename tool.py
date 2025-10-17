import pandas as pd
import os

# 当前目录下的 price.xlsx
file_path = os.path.join(os.getcwd(), "price.xlsx")

if not os.path.exists(file_path):
    raise FileNotFoundError(f"未找到文件：{file_path}")

# 读取 Excel
df = pd.read_excel(file_path)

# 判断是否有“品类”列
if "品类" not in df.columns:
    raise ValueError("文件中未找到“品类”这一列。")

# 统计不同品类的数量
unique_categories = df["品类"].dropna().unique()
count = len(unique_categories)

print(f"共有 {count} 个不同的“品类”。")
print("分别是：")
for c in unique_categories:
    print("-", c)
