import pandas as pd
import os

# ① 设置文件路径
input_path = r"D:\JAVA\code\pdd_project\filtered_result.xlsx"
output_path = r"D:\JAVA\code\pdd_project\filtered_result_positive.xlsx"

# ② 检查文件
if not os.path.exists(input_path):
    raise FileNotFoundError(f"❌ 未找到文件：{input_path}")

# ③ 读取数据
df = pd.read_excel(input_path)

# ④ 确保“差额”列为数值型
df['差额'] = pd.to_numeric(df['差额'], errors='coerce')

# ⑤ 过滤掉差额 < 0 的行
df_filtered = df[df['差额'] >= 0]

# ⑥ 输出结果
if df_filtered.empty:
    print("⚠️ 过滤后没有符合条件的数据。")
else:
    df_filtered.to_excel(output_path, index=False, sheet_name="已过滤结果")
    print(f"✅ 已过滤掉差额 < 0 的商品，共保留 {len(df_filtered)} 条。")
    print(f"📂 新文件已保存到：{output_path}")