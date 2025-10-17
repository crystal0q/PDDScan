import time, hashlib, requests, json

client_id = "98a8c70b50294461bd62930e9d9686cc"
client_secret = "f7bd3900d6def8e9d12dd198cb7838eedb1d527b"
pid="43512226_310483284"

custom_parameters = '{"uid":"demo","source":"test"}'

url = "https://gw-api.pinduoduo.com/api/router"

def sign(params):
    items = sorted(params.items())
    raw = client_secret + "".join(f"{k}{v}" for k, v in items) + client_secret
    return hashlib.md5(raw.encode("utf-8")).hexdigest().upper()

def search_goods(keyword, page=1, page_size=50):
    params = {
        "type": "pdd.ddk.goods.search",
        "client_id": client_id,
        "timestamp": int(time.time()),
        "data_type": "JSON",
        "keyword": keyword,
        "pid": pid,
        "custom_parameters": custom_parameters,
        "page": page,
        "page_size": page_size
    }
    params["sign"] = sign(params)
    r = requests.post(url, data=params)
    try:
        return r.json()
    except:
        print("⚠️ 返回不是 JSON：", r.text)
        return {}

def main():
    keyword = "伊利 黄桃 燕麦奶"
    all_goods = []

    # 翻页获取前 3 页
    for page in range(1, 4):
        res = search_goods(keyword, page=page, page_size=50)
        # 错误提示辅助：若有公共参数错误等，打印出来
        if "error_response" in res:
            print("接口错误：", res["error_response"])
            break

        goods_list = res.get("goods_search_response", {}).get("goods_list", [])
        if not goods_list:
            break
        all_goods.extend(goods_list)

    if not all_goods:
        print("❌ 没有搜索到商品")
        return

    print(f"\n✅ 价格低于 ¥50 的『{keyword}』商品ID（从前 {len(all_goods)} 条中筛选）：\n")
    for g in all_goods:
        price = g.get("min_group_price", 0) / 100
        if price < 50:
            goods_id = g.get("goods_id")
            if goods_id is not None:
                print(f"- goods_id: {goods_id}")

if __name__ == "__main__":
    main()